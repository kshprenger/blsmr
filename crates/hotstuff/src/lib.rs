use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use client::{CmdId, Command};
use dscale::{helpers::Quorum, services::kv, *};
use rustc_hash::FxHashMap;

pub const B0: &str = "hotstuff_genesis";
pub const HOTSTUFF_POOL: &str = dscale::GLOBAL_POOL;
pub const KEY_BLOCK_COMMITS: &str = "hotstuff_block_commits";
pub const KEY_LATENCIES: &str = "hotstuff_latencies";
pub const KEY_SUBMIT_INTERVAL: &str = "submit_interval";
pub const KEY_SUBMIT_LIMIT: &str = "submit_limit";

type NodeId = usize;

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub parent: Option<Weak<Node>>,
    pub height: usize,
    commands: Arc<[Arc<SubmittedCommand>]>,
}

impl Node {
    pub fn genesis() -> Self {
        Self {
            id: 0,
            parent: None,
            height: 0,
            commands: Arc::from([]),
        }
    }

    fn parent(&self) -> Option<Arc<Self>> {
        self.parent.as_ref()?.upgrade()
    }
}

#[derive(Debug)]
struct SubmittedCommand {
    command: Command,
    submitted_at: Jiffies,
    committed: AtomicBool,
}

#[derive(Debug)]
enum HSMessage {
    Submit(Arc<SubmittedCommand>),
    Propose(Arc<Node>),
    Vote(Arc<Node>),
}

impl Message for HSMessage {}

pub fn is_command_submission(message: &MessagePtr) -> bool {
    matches!(
        message.try_as_type::<HSMessage>(),
        Some(HSMessage::Submit(_))
    )
}

pub struct Hotstuff<const ROTATING: bool> {
    submit_interval: Jiffies,
    submit_limit: usize,
    submitted: usize,
    current_submit_timer_id: TimerId,
    block_commits: Arc<AtomicUsize>,
    pending_commands: FxHashMap<CmdId, Arc<SubmittedCommand>>,
    pending_quorums: FxHashMap<NodeId, (usize, Quorum<()>)>,
    nodes: FxHashMap<NodeId, Arc<Node>>,
    vheight: usize,
    b_lock: Arc<Node>,
    b_exec: Arc<Node>,
    b_leaf: Arc<Node>,
}

pub type ChainedHotstuff = Hotstuff<true>;
pub type NonRotatingHotstuff = Hotstuff<false>;

impl<const ROTATING: bool> Default for Hotstuff<ROTATING> {
    fn default() -> Self {
        let genesis_node = kv::get::<Arc<Node>>(B0);
        let nodes = FxHashMap::from_iter([(genesis_node.id, genesis_node.clone())]);
        Self {
            submit_interval: kv::get(KEY_SUBMIT_INTERVAL),
            submit_limit: kv::get(KEY_SUBMIT_LIMIT),
            submitted: 0,
            current_submit_timer_id: 0,
            block_commits: kv::get(KEY_BLOCK_COMMITS),
            pending_commands: FxHashMap::default(),
            pending_quorums: FxHashMap::default(),
            nodes,
            vheight: 0,
            b_lock: genesis_node.clone(),
            b_exec: genesis_node.clone(),
            b_leaf: genesis_node,
        }
    }
}

impl<const ROTATING: bool> Process for Hotstuff<ROTATING> {
    fn on_start(&mut self) {
        if self.submit_limit != 0 {
            self.schedule_submit();
        }
        if pid() == 0 {
            broadcast(HSMessage::Propose(self.create_leaf()));
        }
    }

    fn on_message(&mut self, _from: Pid, message: MessagePtr) {
        match message.as_type::<HSMessage>() {
            HSMessage::Submit(command) => {
                self.pending_commands
                    .entry(command.command.id)
                    .or_insert_with(|| command.clone());
            }
            HSMessage::Propose(node) => {
                if node.height > self.vheight
                    && (self.extends(node) || node.height > self.b_lock.height)
                {
                    for command in node.commands.iter() {
                        self.pending_commands.remove(&command.command.id);
                    }
                    self.nodes.insert(node.id, node.clone());
                    self.vheight = node.height;
                    send(self.next_leader(), HSMessage::Vote(node.clone()));
                    self.update(node.clone());
                }
            }
            HSMessage::Vote(node) => {
                if node.height <= self.b_exec.height {
                    return;
                }
                let quorum_size = self.quorum_size();
                let (_, quorum) = self
                    .pending_quorums
                    .entry(node.id)
                    .or_insert_with(|| (node.height, Quorum::new(quorum_size)));
                if quorum.add(()).is_some() {
                    self.nodes.insert(node.id, node.clone());
                    self.b_leaf = node.clone();
                    let leaf = self.create_leaf();
                    self.update(leaf.clone());
                    broadcast(HSMessage::Propose(leaf));
                }
            }
        }
    }

    fn on_timer(&mut self, id: TimerId) {
        assert_eq!(id, self.current_submit_timer_id, "unknown timer");
        let command = Arc::new(SubmittedCommand {
            command: client::create_cmd_for_key(0),
            submitted_at: now(),
            committed: AtomicBool::new(false),
        });
        self.pending_commands
            .insert(command.command.id, command.clone());
        let remotes = list_pool(HOTSTUFF_POOL)
            .into_iter()
            .filter(|replica| *replica != pid())
            .collect::<Vec<_>>();
        send_many(&remotes, HSMessage::Submit(command));
        self.submitted += 1;
        if self.submitted < self.submit_limit {
            self.schedule_submit();
        }
    }
}

impl<const ROTATING: bool> Hotstuff<ROTATING> {
    fn update(&mut self, node: Arc<Node>) {
        let grandparent = node.parent().and_then(|parent| parent.parent());
        let great_grandparent = grandparent.as_ref().and_then(|parent| parent.parent());

        if node.height > self.b_leaf.height {
            self.b_leaf = node;
        }
        if let Some(grandparent) = &grandparent
            && grandparent.height > self.b_lock.height
        {
            self.b_lock = grandparent.clone();
        }
        if let Some(block) = great_grandparent {
            self.commit(block.clone());
            self.b_exec = block;
            self.collect_garbage();
        }
    }

    fn commit(&mut self, node: Arc<Node>) {
        if self.b_exec.height < node.height {
            self.commit(node.parent().expect("genesis cannot commit"));
            self.block_commits.fetch_add(1, Ordering::Relaxed);
            for command in node.commands.iter() {
                if !command.committed.swap(true, Ordering::Relaxed) {
                    let latency = now() - command.submitted_at;
                    kv::modify::<Vec<Jiffies>>(KEY_LATENCIES, |latencies| {
                        latencies.push(latency);
                    });
                }
            }
        }
    }

    fn schedule_submit(&mut self) {
        self.current_submit_timer_id = schedule_timer_after(self.submit_interval);
    }

    fn create_leaf(&mut self) -> Arc<Node> {
        let parent = self.b_leaf.clone();
        let node = Arc::new(Node {
            id: unique_id(),
            parent: Some(Arc::downgrade(&parent)),
            height: parent.height + 1,
            commands: self
                .pending_commands
                .drain()
                .map(|(_, command)| command)
                .collect::<Vec<_>>()
                .into(),
        });
        self.nodes.insert(node.id, node.clone());
        node
    }

    fn collect_garbage(&mut self) {
        let committed_height = self.b_exec.height;
        self.nodes.retain(|_, node| node.height >= committed_height);
        self.pending_quorums
            .retain(|_, (height, _)| *height > committed_height);
    }

    fn next_leader(&self) -> Pid {
        let replicas = list_pool(HOTSTUFF_POOL);
        if ROTATING {
            replicas[self.vheight % replicas.len()]
        } else {
            replicas[0]
        }
    }

    fn quorum_size(&self) -> usize {
        (list_pool(HOTSTUFF_POOL).len() * 2) / 3 + 1
    }

    fn extends(&self, node: &Arc<Node>) -> bool {
        node.parent()
            .is_some_and(|parent| parent.id == self.b_lock.id)
    }
}
