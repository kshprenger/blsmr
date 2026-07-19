use std::{collections::HashMap, sync::Arc};

use dscale::{helpers::Quorum, services::kv, *};

pub const B0: &str = "hotstuff_genesis";
pub const HOTSTUFF_POOL: &str = dscale::GLOBAL_POOL;
pub const KEY_LATENCIES: &str = "hotstuff_latencies";

type NodeId = usize;

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub parent: Option<Arc<Node>>,
    pub height: usize,
}

#[derive(Debug)]
pub enum HSMessage {
    Propose(Arc<Node>),
    Vote(Arc<Node>),
}

impl Message for HSMessage {}

pub struct Hotstuff<const ROTATING: bool> {
    pending_quorums: HashMap<NodeId, Quorum<()>>,
    observed_at: HashMap<NodeId, Jiffies>,
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
        Self {
            pending_quorums: HashMap::new(),
            observed_at: HashMap::new(),
            vheight: 0,
            b_lock: genesis_node.clone(),
            b_exec: genesis_node.clone(),
            b_leaf: genesis_node,
        }
    }
}

impl<const ROTATING: bool> Process for Hotstuff<ROTATING> {
    fn on_start(&mut self) {
        if pid() == 0 {
            broadcast(HSMessage::Propose(self.create_leaf()));
        }
    }

    fn on_message(&mut self, _from: Pid, message: MessagePtr) {
        match message.as_type::<HSMessage>() {
            HSMessage::Propose(node) => {
                self.observed_at.entry(node.id).or_insert_with(now);
                if node.height > self.vheight
                    && (self.extends(node) || node.height > self.b_lock.height)
                {
                    self.vheight = node.height;
                    send(self.next_leader(), HSMessage::Vote(node.clone()));
                    self.update(node.clone());
                }
            }
            HSMessage::Vote(node) => {
                let quorum_size = self.quorum_size();
                let quorum = self
                    .pending_quorums
                    .entry(node.id)
                    .or_insert_with(|| Quorum::new(quorum_size));
                if quorum.add(()).is_some() {
                    self.b_leaf = node.clone();
                    broadcast(HSMessage::Propose(self.create_leaf()));
                }
            }
        }
    }

    fn on_timer(&mut self, _id: TimerId) {
        unreachable!()
    }
}

impl<const ROTATING: bool> Hotstuff<ROTATING> {
    fn update(&mut self, node: Arc<Node>) {
        let grandparent = node
            .parent
            .as_ref()
            .and_then(|parent| parent.parent.clone());
        let great_grandparent = grandparent
            .as_ref()
            .and_then(|parent| parent.parent.clone());

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
        }
    }

    fn commit(&mut self, node: Arc<Node>) {
        if self.b_exec.height < node.height {
            self.commit(node.parent.clone().expect("genesis cannot commit"));
            if let Some(observed_at) = self.observed_at.remove(&node.id) {
                kv::modify::<Vec<Jiffies>>(KEY_LATENCIES, |latencies| {
                    latencies.push(now() - observed_at);
                });
            }
        }
    }

    fn create_leaf(&mut self) -> Arc<Node> {
        let parent = self.b_leaf.clone();
        let node = Arc::new(Node {
            id: unique_id(),
            parent: Some(parent.clone()),
            height: parent.height + 1,
        });
        self.observed_at.insert(node.id, now());
        node
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
        Arc::ptr_eq(
            node.parent.as_ref().expect("proposal has parent"),
            &self.b_lock,
        )
    }
}
