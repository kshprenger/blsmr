mod consistent_broadcast;
mod dag_utils;

use std::{
    collections::BTreeSet,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicUsize},
    },
};

use client::{CmdId, Command};
use dscale::*;
use rustc_hash::FxHashMap;

use crate::{
    consistent_broadcast::{BCBMessage, ByzantineConsistentBroadcast},
    dag_utils::{RoundBasedDAG, Vertex, VertexMessage, VertexPtr, VertexType, same_vertex},
};

pub const BULLSHARK_POOL: &str = dscale::GLOBAL_POOL;
pub const KEY_BLOCK_COMMITS: &str = "bullshark_block_commits";
pub const KEY_LATENCIES: &str = "bullshark_latencies";
pub const KEY_SUBMIT_INTERVAL: &str = "submit_interval";
pub const KEY_SUBMIT_LIMIT: &str = "submit_limit";

#[derive(Debug)]
pub(crate) struct SubmittedCommand {
    pub(crate) command: Command,
    pub(crate) submitted_at: Jiffies,
    pub(crate) committed: AtomicBool,
}

#[derive(Debug)]
enum BullsharkMessage {
    Submit(Arc<SubmittedCommand>),
}

impl Message for BullsharkMessage {}

pub fn is_command_submission(message: &MessagePtr) -> bool {
    matches!(
        message.try_as_type::<BullsharkMessage>(),
        Some(BullsharkMessage::Submit(_))
    )
}

pub struct Bullshark {
    submit_interval: Jiffies,
    submit_limit: usize,
    submitted: usize,
    submit_timer: TimerId,
    block_commits: Arc<AtomicUsize>,
    pending_commands: FxHashMap<CmdId, Arc<SubmittedCommand>>,
    rbcast: ByzantineConsistentBroadcast,
    proc_num: usize,
    dag: RoundBasedDAG,
    round: usize,
    buffer: BTreeSet<VertexPtr>,
    last_ordered_round: usize,
    ordered_anchors: Vec<VertexPtr>,
    wait: bool,
    timer: TimerId,
}

impl Default for Bullshark {
    fn default() -> Self {
        Self {
            submit_interval: services::kv::get(KEY_SUBMIT_INTERVAL),
            submit_limit: services::kv::get(KEY_SUBMIT_LIMIT),
            submitted: 0,
            submit_timer: 0,
            block_commits: services::kv::get(KEY_BLOCK_COMMITS),
            pending_commands: FxHashMap::default(),
            rbcast: ByzantineConsistentBroadcast::default(),
            proc_num: 0,
            dag: RoundBasedDAG::default(),
            round: 0,
            buffer: BTreeSet::new(),
            last_ordered_round: 0,
            ordered_anchors: Vec::new(),
            wait: true,
            timer: 0,
        }
    }
}

impl Process for Bullshark {
    fn on_start(&mut self) {
        if self.submit_limit != 0 {
            self.schedule_submit();
        }
        self.proc_num = list_pool(BULLSHARK_POOL).len();
        self.dag.set_round_size(self.proc_num);
        self.rbcast.on_start(self.proc_num);
        self.rbcast.reliably_broadcast(VertexMessage {
            proc_num: self.proc_num,
            typ: VertexType::Genesis(VertexPtr::new(Vertex {
                round: 0,
                source: pid(),
                strong_edges: Vec::new(),
                commands: Arc::from([]),
            })),
        });
    }

    fn on_message(&mut self, from: Pid, message: MessagePtr) {
        if let Some(BullsharkMessage::Submit(command)) = message.try_as_type::<BullsharkMessage>() {
            self.pending_commands
                .entry(command.command.id)
                .or_insert_with(|| command.clone());
            return;
        }
        let Some(message) = self
            .rbcast
            .on_message(from, message.as_type::<BCBMessage>())
        else {
            return;
        };
        match &message.as_type::<VertexMessage>().typ {
            VertexType::Genesis(vertex) => {
                self.dag.add_vertex(vertex.clone());
                self.try_advance_round();
            }
            VertexType::Vertex(vertex) => {
                if self.bad_vertex(vertex, from) {
                    return;
                }
                for command in vertex.commands.iter() {
                    self.pending_commands.remove(&command.command.id);
                }
                let mut buffered = self.buffer.iter().cloned().collect::<Vec<_>>();
                buffered.sort_by_key(|vertex| vertex.round);
                for vertex in buffered {
                    self.try_add_to_dag(vertex);
                }
                if !self.try_add_to_dag(vertex.clone()) {
                    self.buffer.insert(vertex.clone());
                }
                if self.round != vertex.round {
                    return;
                }
                if !self.wait {
                    self.try_advance_round();
                    return;
                }
                match self.round % 4 {
                    0 | 2 => {
                        if self.get_anchor(self.round).is_some() {
                            self.try_advance_round();
                        }
                    }
                    1 | 3 => {
                        let Some(anchor) = self.get_anchor(self.round - 1) else {
                            return;
                        };
                        let anchor = Arc::as_ptr(&anchor);
                        if self.dag[self.round]
                            .iter()
                            .flatten()
                            .filter(|vertex| {
                                vertex
                                    .strong_edges
                                    .iter()
                                    .any(|edge| edge.as_ptr() == anchor)
                            })
                            .count()
                            >= self.quorum_size()
                        {
                            self.try_advance_round();
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn on_timer(&mut self, id: TimerId) {
        if id == self.submit_timer {
            let command = Arc::new(SubmittedCommand {
                command: client::create_cmd_for_key(0),
                submitted_at: now(),
                committed: AtomicBool::new(false),
            });
            broadcast(BullsharkMessage::Submit(command));
            self.submitted += 1;
            if self.submitted < self.submit_limit {
                self.schedule_submit();
            }
        } else if id == self.timer {
            self.wait = false;
            self.try_advance_round();
        }
    }
}

impl Bullshark {
    fn adversary_threshold(&self) -> usize {
        (self.proc_num - 1) / 3
    }

    fn quorum_size(&self) -> usize {
        2 * self.adversary_threshold() + 1
    }

    fn direct_commit_threshold(&self) -> usize {
        self.adversary_threshold() + 1
    }

    fn quorum_reached_for_round(&self, round: usize) -> bool {
        self.dag[round].iter().flatten().count() >= self.quorum_size()
    }

    fn schedule_submit(&mut self) {
        self.submit_timer = schedule_timer_after(self.submit_interval);
    }

    fn create_vertex(&mut self, round: usize) -> VertexPtr {
        VertexPtr::new(Vertex {
            round,
            source: pid(),
            strong_edges: self.dag[round - 1]
                .iter()
                .flatten()
                .map(Arc::downgrade)
                .collect::<Vec<Weak<Vertex>>>(),
            commands: self
                .pending_commands
                .drain()
                .map(|(_, command)| command)
                .collect::<Vec<_>>()
                .into(),
        })
    }

    fn bad_vertex(&self, vertex: &VertexPtr, from: Pid) -> bool {
        vertex.strong_edges.len() < self.quorum_size() || from != vertex.source
    }

    fn get_leader_id(&self, round: usize) -> Pid {
        round % self.proc_num + 1
    }

    fn get_anchor(&self, round: usize) -> Option<VertexPtr> {
        self.dag[round][self.get_leader_id(round)].clone()
    }

    fn start_timer(&mut self) {
        self.timer = schedule_timer_after(Jiffies(10_000));
        self.wait = true;
    }

    fn try_advance_round(&mut self) {
        if self.quorum_reached_for_round(self.round) {
            self.round += 1;
            self.start_timer();
            self.broadcast_vertex(self.round);
        }
    }

    fn broadcast_vertex(&mut self, round: usize) {
        let vertex = self.create_vertex(round);
        self.try_add_to_dag(vertex.clone());
        self.rbcast.reliably_broadcast(VertexMessage {
            proc_num: self.proc_num,
            typ: VertexType::Vertex(vertex),
        });
    }

    fn try_add_to_dag(&mut self, vertex: VertexPtr) -> bool {
        if vertex.round <= self.dag.min_round() {
            self.buffer.remove(&vertex);
            return true;
        }
        if vertex.round - 1 > self.dag.current_max_allocated_round() {
            return false;
        }
        if !vertex
            .strong_edges
            .iter()
            .map(|edge| edge.upgrade().expect("live strong edge"))
            .all(|edge| match self.dag[edge.round][edge.source] {
                None => false,
                Some(ref known) => same_vertex(&edge, known),
            })
        {
            return false;
        }
        self.dag.add_vertex(vertex.clone());
        if self.quorum_reached_for_round(vertex.round) && vertex.round > self.round {
            self.round = vertex.round;
            self.start_timer();
            self.broadcast_vertex(vertex.round);
        }
        self.buffer.remove(&vertex);
        if vertex.source == self.get_leader_id(vertex.round) {
            self.try_ordering(vertex);
        }
        true
    }

    fn try_ordering(&mut self, vertex: VertexPtr) {
        if vertex.round % 2 == 1 || vertex.round == 0 {
            return;
        }
        let Some(anchor) = self.get_anchor(vertex.round - 2) else {
            return;
        };
        let anchor_ptr = Arc::as_ptr(&anchor);
        let votes = vertex
            .strong_edges
            .iter()
            .map(|edge| edge.upgrade().expect("live strong edge"))
            .filter(|vote| {
                vote.strong_edges
                    .iter()
                    .any(|edge| edge.as_ptr() == anchor_ptr)
            })
            .count();
        if votes >= self.direct_commit_threshold() {
            self.order_anchors(anchor);
        }
    }

    fn order_anchors(&mut self, vertex: VertexPtr) {
        let mut anchor = vertex.clone();
        self.ordered_anchors.push(anchor.clone());
        let mut round = anchor.round.saturating_sub(2);
        while round > self.last_ordered_round {
            match self.get_anchor(round) {
                None => round -= 2,
                Some(previous) => {
                    if self.dag.path_exists(&anchor, &previous) {
                        self.ordered_anchors.push(previous.clone());
                        anchor = previous;
                    }
                    round -= 2;
                }
            }
        }
        self.last_ordered_round = vertex.round;
        while let Some(anchor) = self.ordered_anchors.pop() {
            self.dag.order_from(&anchor, &self.block_commits);
        }
        self.dag.gc(self.last_ordered_round);
    }
}
