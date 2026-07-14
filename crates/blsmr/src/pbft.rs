use std::collections::HashSet;

use client::CmdId;
use dscale::services::kv;
use rustc_hash::FxHashMap;

use crate::{KEY_QUORUM_SYSTEM, POOL_BLSMR};

#[derive(Debug, Clone)]
pub struct PrePrepare {
    pub cmd_id: CmdId,
    pub deps: Vec<CmdId>,
}

#[derive(Debug, Clone)]
pub struct Prepare {
    pub cmd_id: CmdId,
    pub deps: Vec<CmdId>,
}

#[derive(Debug, Clone)]
pub struct Commit {
    pub cmd_id: CmdId,
    pub deps: Vec<CmdId>,
}

#[derive(Debug)]
pub enum PbftMsg {
    PrePrepare(PrePrepare),
    Prepare(Prepare),
    Commit(Commit),
}

impl dscale::Message for PbftMsg {}

pub enum PbftStatus {
    DoNothing,
    Decided(CmdId, Vec<CmdId>),
}

struct Instance {
    deps: Vec<CmdId>,
    sent_prepare: bool,
    prepares: HashSet<dscale::Pid>,
    sent_commit: bool,
    commits: HashSet<dscale::Pid>,
    decided: bool,
}

impl Instance {
    fn new(deps: Vec<CmdId>) -> Self {
        Self {
            deps,
            sent_prepare: false,
            prepares: HashSet::new(),
            sent_commit: false,
            commits: HashSet::new(),
            decided: false,
        }
    }
}

pub(crate) struct Pbft {
    bqs: quorum::QuorumSystem,
    instances: FxHashMap<CmdId, Instance>,
}

impl Default for Pbft {
    fn default() -> Self {
        Self {
            bqs: kv::get::<quorum::QuorumSystem>(KEY_QUORUM_SYSTEM),
            instances: FxHashMap::default(),
        }
    }
}

impl Pbft {
    pub(crate) fn propose(&mut self, cmd_id: CmdId, deps: Vec<CmdId>) {
        dscale::broadcast_within_pool(POOL_BLSMR, PbftMsg::PrePrepare(PrePrepare { cmd_id, deps }));
    }

    pub(crate) fn on_message(&mut self, from: dscale::Pid, message: &PbftMsg) -> PbftStatus {
        match message {
            PbftMsg::PrePrepare(m) => {
                let instance = self
                    .instances
                    .entry(m.cmd_id)
                    .or_insert_with(|| Instance::new(m.deps.clone()));
                if !instance.sent_prepare {
                    instance.sent_prepare = true;
                    let deps = instance.deps.clone();
                    dscale::broadcast_within_pool(
                        POOL_BLSMR,
                        PbftMsg::Prepare(Prepare {
                            cmd_id: m.cmd_id,
                            deps,
                        }),
                    );
                }
                PbftStatus::DoNothing
            }
            PbftMsg::Prepare(m) => {
                let instance = self
                    .instances
                    .entry(m.cmd_id)
                    .or_insert_with(|| Instance::new(m.deps.clone()));
                instance.prepares.insert(from);
                if !instance.sent_commit && self.bqs.is_quorum(instance.prepares.iter().copied()) {
                    instance.sent_commit = true;
                    let deps = instance.deps.clone();
                    dscale::broadcast_within_pool(
                        POOL_BLSMR,
                        PbftMsg::Commit(Commit {
                            cmd_id: m.cmd_id,
                            deps,
                        }),
                    );
                }
                PbftStatus::DoNothing
            }
            PbftMsg::Commit(m) => {
                let instance = self
                    .instances
                    .entry(m.cmd_id)
                    .or_insert_with(|| Instance::new(m.deps.clone()));
                instance.commits.insert(from);
                if !instance.decided && self.bqs.is_quorum(instance.commits.iter().copied()) {
                    instance.decided = true;
                    let deps = std::mem::take(&mut instance.deps);
                    instance.prepares = HashSet::new();
                    instance.commits = HashSet::new();
                    return PbftStatus::Decided(m.cmd_id, deps);
                }
                PbftStatus::DoNothing
            }
        }
    }
}
