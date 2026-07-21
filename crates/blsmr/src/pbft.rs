use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use client::CmdId;
use dscale::services::kv;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use crate::{BLSMRProtocol, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, POOL_BLSMR};

#[derive(Debug, Clone)]
pub struct PrePrepare {
    pub cmd_id: CmdId,
    pub deps: Arc<[CmdId]>,
    pub committee: Arc<[dscale::Pid]>,
}

#[derive(Debug, Clone)]
pub struct Prepare {
    pub cmd_id: CmdId,
    pub deps: Arc<[CmdId]>,
    pub committee: Arc<[dscale::Pid]>,
}

#[derive(Debug, Clone)]
pub struct Commit {
    pub cmd_id: CmdId,
    pub deps: Arc<[CmdId]>,
    pub committee: Arc<[dscale::Pid]>,
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
    Decided(CmdId, Arc<[CmdId]>, dscale::Pid),
}

struct Instance {
    deps: Arc<[CmdId]>,
    committee: Arc<[dscale::Pid]>,
    sent_prepare: bool,
    prepares: FxHashSet<dscale::Pid>,
    sent_commit: bool,
    commits: FxHashSet<dscale::Pid>,
    decided: bool,
}

impl Instance {
    fn new(deps: Arc<[CmdId]>, committee: Arc<[dscale::Pid]>) -> Self {
        Self {
            deps,
            committee,
            sent_prepare: false,
            prepares: FxHashSet::default(),
            sent_commit: false,
            commits: FxHashSet::default(),
            decided: false,
        }
    }
}

pub(crate) struct Pbft {
    protocol_type: BLSMRProtocol,
    bqs: quorum::QuorumSystem,
    instances: FxHashMap<CmdId, Instance>,
    decided: FxHashSet<CmdId>,
}

impl Default for Pbft {
    fn default() -> Self {
        Self {
            protocol_type: kv::get::<BLSMRProtocol>(KEY_PROTOCOL_TYPE),
            bqs: kv::get::<quorum::QuorumSystem>(KEY_QUORUM_SYSTEM),
            instances: FxHashMap::default(),
            decided: FxHashSet::default(),
        }
    }
}

impl Pbft {
    pub(crate) fn propose(&mut self, cmd_id: CmdId, deps: Arc<[CmdId]>) {
        let committee: Arc<[dscale::Pid]> = self.committee(cmd_id).into();
        let message = PbftMsg::PrePrepare(PrePrepare {
            cmd_id,
            deps,
            committee: Arc::clone(&committee),
        });
        self.send(&committee, message);
    }

    pub(crate) fn on_message(&mut self, from: dscale::Pid, message: &PbftMsg) -> PbftStatus {
        match message {
            PbftMsg::PrePrepare(m) => {
                if self.decided.contains(&m.cmd_id) {
                    return PbftStatus::DoNothing;
                }
                let prepare = {
                    let instance = self.instances.entry(m.cmd_id).or_insert_with(|| {
                        Instance::new(Arc::clone(&m.deps), Arc::clone(&m.committee))
                    });
                    if instance.sent_prepare {
                        None
                    } else {
                        instance.sent_prepare = true;
                        Some((Arc::clone(&instance.deps), Arc::clone(&instance.committee)))
                    }
                };
                if let Some((deps, committee)) = prepare {
                    let message = PbftMsg::Prepare(Prepare {
                        cmd_id: m.cmd_id,
                        deps,
                        committee: Arc::clone(&committee),
                    });
                    self.send(&committee, message);
                }
                PbftStatus::DoNothing
            }
            PbftMsg::Prepare(m) => {
                let is_three_jane = matches!(&self.protocol_type, BLSMRProtocol::ThreeJane);
                if self.decided.contains(&m.cmd_id)
                    || !accepts_sender(&self.protocol_type, &m.committee, from)
                {
                    return PbftStatus::DoNothing;
                }
                let bqs = &self.bqs;
                let commit = {
                    let instance = self.instances.entry(m.cmd_id).or_insert_with(|| {
                        Instance::new(Arc::clone(&m.deps), Arc::clone(&m.committee))
                    });
                    instance.prepares.insert(from);
                    let ready = if is_three_jane {
                        instance.prepares.len() >= committee_quorum(instance.committee.len())
                    } else {
                        bqs.is_quorum(instance.prepares.iter().copied())
                    };
                    if !instance.sent_commit && ready {
                        instance.sent_commit = true;
                        Some((Arc::clone(&instance.deps), Arc::clone(&instance.committee)))
                    } else {
                        None
                    }
                };
                if let Some((deps, committee)) = commit {
                    let message = PbftMsg::Commit(Commit {
                        cmd_id: m.cmd_id,
                        deps,
                        committee: Arc::clone(&committee),
                    });
                    self.send(&committee, message);
                }
                PbftStatus::DoNothing
            }
            PbftMsg::Commit(m) => {
                let is_three_jane = matches!(&self.protocol_type, BLSMRProtocol::ThreeJane);
                if self.decided.contains(&m.cmd_id)
                    || !accepts_sender(&self.protocol_type, &m.committee, from)
                {
                    return PbftStatus::DoNothing;
                }
                let bqs = &self.bqs;
                let decision = {
                    let instance = self.instances.entry(m.cmd_id).or_insert_with(|| {
                        Instance::new(Arc::clone(&m.deps), Arc::clone(&m.committee))
                    });
                    instance.commits.insert(from);
                    let ready = if is_three_jane {
                        instance.commits.len() >= committee_quorum(instance.committee.len())
                    } else {
                        bqs.is_quorum(instance.commits.iter().copied())
                    };
                    if !instance.decided && ready {
                        instance.decided = true;
                        let leader = instance.committee.first().copied().unwrap_or(m.cmd_id.pid);
                        Some((Arc::clone(&instance.deps), leader))
                    } else {
                        None
                    }
                };
                if let Some((deps, leader)) = decision {
                    self.instances.remove(&m.cmd_id);
                    self.decided.insert(m.cmd_id);
                    return PbftStatus::Decided(m.cmd_id, deps, leader);
                }
                PbftStatus::DoNothing
            }
        }
    }

    fn committee(&self, cmd_id: CmdId) -> Vec<dscale::Pid> {
        if !matches!(&self.protocol_type, BLSMRProtocol::ThreeJane) {
            return Vec::new();
        }
        let mut committee = dscale::list_pool(POOL_BLSMR);
        committee.sort_by_key(|pid| {
            let mut hasher = FxHasher::default();
            cmd_id.hash(&mut hasher);
            pid.hash(&mut hasher);
            hasher.finish()
        });
        committee.truncate(self.bqs.consensus_committee_size());
        committee
    }

    fn send(&self, committee: &[dscale::Pid], message: PbftMsg) {
        if matches!(&self.protocol_type, BLSMRProtocol::ThreeJane) {
            dscale::send_many(committee, message);
        } else {
            dscale::broadcast_within_pool(POOL_BLSMR, message);
        }
    }
}

fn committee_quorum(size: usize) -> usize {
    2 * ((size - 1) / 3) + 1
}

fn accepts_sender(
    protocol_type: &BLSMRProtocol,
    committee: &[dscale::Pid],
    from: dscale::Pid,
) -> bool {
    !matches!(protocol_type, BLSMRProtocol::ThreeJane) || committee.contains(&from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wintermute_accepts_sender_without_committee() {
        assert!(accepts_sender(&BLSMRProtocol::Wintermute, &[], 7));
    }

    #[test]
    fn three_jane_accepts_only_committee_members() {
        assert!(accepts_sender(&BLSMRProtocol::ThreeJane, &[1, 2], 2));
        assert!(!accepts_sender(&BLSMRProtocol::ThreeJane, &[1, 2], 3));
    }
}
