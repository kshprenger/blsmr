use std::sync::{Arc, Weak};

use dscale::{helpers::Quorum, services::kv, *};
use rustc_hash::FxHashMap;

pub const B0: &str = "hotstuff_genesis";
pub const HOTSTUFF_POOL: &str = dscale::GLOBAL_POOL;
pub const KEY_LATENCIES: &str = "hotstuff_latencies";

type NodeId = usize;

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub parent: Option<Weak<Node>>,
    pub height: usize,
}

impl Node {
    fn parent(&self) -> Option<Arc<Self>> {
        self.parent.as_ref()?.upgrade()
    }
}

#[derive(Debug)]
pub enum HSMessage {
    Propose(Arc<Node>),
    Vote(Arc<Node>),
}

impl Message for HSMessage {}

pub struct Hotstuff<const ROTATING: bool> {
    pending_quorums: FxHashMap<NodeId, (usize, Quorum<()>)>,
    observed_at: FxHashMap<NodeId, Jiffies>,
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
            pending_quorums: FxHashMap::default(),
            observed_at: FxHashMap::default(),
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
            parent: Some(Arc::downgrade(&parent)),
            height: parent.height + 1,
        });
        self.observed_at.insert(node.id, now());
        self.nodes.insert(node.id, node.clone());
        node
    }

    fn collect_garbage(&mut self) {
        let committed_height = self.b_exec.height;
        self.nodes.retain(|_, node| node.height >= committed_height);
        self.pending_quorums
            .retain(|_, (height, _)| *height > committed_height);
        self.observed_at
            .retain(|node_id, _| self.nodes.contains_key(node_id));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn garbage_collection_releases_committed_ancestors() {
        let genesis = Arc::new(Node {
            id: 0,
            parent: None,
            height: 0,
        });
        let committed = Arc::new(Node {
            id: 1,
            parent: Some(Arc::downgrade(&genesis)),
            height: 1,
        });
        let leaf = Arc::new(Node {
            id: 2,
            parent: Some(Arc::downgrade(&committed)),
            height: 2,
        });
        let genesis_weak = Arc::downgrade(&genesis);
        let mut hotstuff = Hotstuff::<false> {
            pending_quorums: FxHashMap::from_iter([
                (0, (0, Quorum::new(1))),
                (2, (2, Quorum::new(1))),
            ]),
            observed_at: FxHashMap::from_iter([(0, Jiffies(0)), (2, Jiffies(0))]),
            nodes: FxHashMap::from_iter([
                (genesis.id, genesis.clone()),
                (committed.id, committed.clone()),
                (leaf.id, leaf.clone()),
            ]),
            vheight: 2,
            b_lock: committed.clone(),
            b_exec: committed,
            b_leaf: leaf,
        };
        drop(genesis);

        hotstuff.collect_garbage();

        assert!(genesis_weak.upgrade().is_none());
        assert_eq!(hotstuff.nodes.len(), 2);
        assert_eq!(hotstuff.pending_quorums.len(), 1);
        assert_eq!(hotstuff.observed_at.len(), 1);
    }
}
