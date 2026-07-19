use std::{
    collections::VecDeque,
    ops::Index,
    sync::{Arc, Weak},
};

use dscale::{Jiffies, Message, Pid, now, pid, services::kv};

use crate::KEY_LATENCIES;

pub type VertexPtr = Arc<Vertex>;
type Round = Vec<Option<VertexPtr>>;

pub fn same_vertex(left: &VertexPtr, right: &VertexPtr) -> bool {
    Arc::ptr_eq(left, right)
}

#[derive(Debug)]
pub struct Vertex {
    pub round: usize,
    pub source: Pid,
    pub creation_time: Jiffies,
    pub strong_edges: Vec<Weak<Vertex>>,
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        (self.round, self.source) == (other.round, other.source)
    }
}

impl Eq for Vertex {}

impl PartialOrd for Vertex {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Vertex {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.round, self.source).cmp(&(other.round, other.source))
    }
}

#[derive(Clone, Debug)]
pub struct VertexMessage {
    pub proc_num: usize,
    pub typ: VertexType,
}

#[derive(Clone, Debug)]
pub enum VertexType {
    Vertex(VertexPtr),
    Genesis(VertexPtr),
}

impl Message for VertexMessage {
    fn virtual_size(&self) -> usize {
        8 + self.proc_num / 8
            + 128
                * match &self.typ {
                    VertexType::Genesis(vertex) | VertexType::Vertex(vertex) => {
                        vertex.strong_edges.len()
                    }
                }
    }
}

#[derive(Default)]
pub struct RoundBasedDAG {
    proc_num: usize,
    matrix: VecDeque<Round>,
    visited: VecDeque<Vec<bool>>,
    ordered: VecDeque<Vec<bool>>,
    gc_offset: usize,
}

impl RoundBasedDAG {
    pub fn set_round_size(&mut self, proc_num: usize) {
        self.proc_num = proc_num;
    }

    pub fn order_from(&mut self, vertex: &VertexPtr) {
        let mut queue = VecDeque::from([vertex.clone()]);
        while let Some(current) = queue.pop_front() {
            for edge in self.live_edges(&current) {
                let round = self.round(edge.round);
                if self.ordered[round][edge.source] {
                    continue;
                }
                self.ordered[round][edge.source] = true;
                if pid() == edge.source {
                    kv::modify::<Vec<Jiffies>>(KEY_LATENCIES, |latencies| {
                        latencies.push(now() - vertex.creation_time);
                    });
                }
                queue.push_back(edge);
            }
        }
    }

    pub fn path_exists(&mut self, from: &VertexPtr, to: &VertexPtr) -> bool {
        if same_vertex(from, to) {
            return true;
        }
        self.reset_visited();
        let from_round = self.round(from.round);
        self.visited[from_round][from.source] = true;
        let mut queue = VecDeque::from([from.clone()]);
        while let Some(current) = queue.pop_front() {
            for edge in self.live_edges(&current) {
                if edge.round < to.round {
                    continue;
                }
                if same_vertex(&edge, to) {
                    return true;
                }
                let round = self.round(edge.round);
                if !self.visited[round][edge.source] {
                    self.visited[round][edge.source] = true;
                    queue.push_back(edge);
                }
            }
        }
        false
    }

    pub fn add_vertex(&mut self, vertex: VertexPtr) {
        if self.current_allocated_rounds() <= vertex.round {
            self.grow(vertex.round + 1 - self.current_allocated_rounds());
        }
        self.insert(vertex);
    }

    pub fn current_max_allocated_round(&self) -> usize {
        self.current_allocated_rounds().saturating_sub(1)
    }

    pub fn min_round(&self) -> usize {
        self.gc_offset
    }

    pub fn gc(&mut self, keep_from: usize) {
        while self.gc_offset < keep_from && self.matrix.len() > 1 {
            self.matrix.pop_front();
            self.visited.pop_front();
            self.ordered.pop_front();
            self.gc_offset += 1;
        }
    }

    fn current_allocated_rounds(&self) -> usize {
        self.matrix.len() + self.gc_offset
    }

    fn live_edges(&self, vertex: &VertexPtr) -> Vec<VertexPtr> {
        vertex
            .strong_edges
            .iter()
            .filter_map(|edge| edge.upgrade())
            .filter(|edge| edge.round >= self.gc_offset)
            .collect()
    }

    fn round(&self, round: usize) -> usize {
        round - self.gc_offset
    }

    fn grow(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.matrix.push_back(vec![None; self.proc_num + 1]);
            self.visited.push_back(vec![false; self.proc_num + 1]);
            self.ordered.push_back(vec![false; self.proc_num + 1]);
        }
    }

    fn insert(&mut self, vertex: VertexPtr) {
        let round = self.round(vertex.round);
        let source = vertex.source;
        self.matrix[round][source] = Some(vertex);
    }

    fn reset_visited(&mut self) {
        for round in &mut self.visited {
            round.fill(false);
        }
    }
}

impl Index<usize> for RoundBasedDAG {
    type Output = Round;

    fn index(&self, index: usize) -> &Self::Output {
        &self.matrix[self.round(index)]
    }
}
