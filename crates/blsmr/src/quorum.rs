use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use client::CmdId;
use dscale::services::kv;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use crate::{BLSMRProtocol, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, POOL_BLSMR};

#[derive(Debug, Clone)]
pub struct Request {
    pub cmd_id: CmdId,
    pub deps: Arc<[CmdId]>,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub cmd_id: CmdId,
    pub deps: Arc<[CmdId]>,
}

#[derive(Debug)]
pub enum QuorumMsg {
    Request(Request),
    Response(Response),
}

impl dscale::Message for QuorumMsg {}

pub enum QuorumStatus {
    DoNothing,
    Decided(CmdId, Arc<[CmdId]>),
}

struct Instance {
    deps: Arc<[CmdId]>,
    replicas: Arc<[dscale::Pid]>,
    responses: FxHashSet<dscale::Pid>,
}

impl Instance {
    fn new(deps: Arc<[CmdId]>, replicas: Arc<[dscale::Pid]>) -> Self {
        Self {
            deps,
            replicas,
            responses: FxHashSet::default(),
        }
    }

    fn accept(&mut self, from: dscale::Pid, deps: &[CmdId]) -> bool {
        self.deps.as_ref() == deps && self.replicas.contains(&from) && {
            self.responses.insert(from);
            self.responses.len() == self.replicas.len()
        }
    }
}

pub(crate) struct Quorum {
    protocol_type: BLSMRProtocol,
    bqs: ::quorum::QuorumSystem,
    instances: FxHashMap<CmdId, Instance>,
}

impl Default for Quorum {
    fn default() -> Self {
        Self {
            protocol_type: kv::get::<BLSMRProtocol>(KEY_PROTOCOL_TYPE),
            bqs: kv::get::<::quorum::QuorumSystem>(KEY_QUORUM_SYSTEM),
            instances: FxHashMap::default(),
        }
    }
}

impl Quorum {
    pub(crate) fn propose(&mut self, cmd_id: CmdId, deps: Arc<[CmdId]>) {
        let replicas: Arc<[dscale::Pid]> = self.replicas(cmd_id).into();
        self.instances.insert(
            cmd_id,
            Instance::new(Arc::clone(&deps), Arc::clone(&replicas)),
        );
        let message = QuorumMsg::Request(Request { cmd_id, deps });
        dscale::send_many(&replicas, message);
    }

    pub(crate) fn on_message(&mut self, from: dscale::Pid, message: &QuorumMsg) -> QuorumStatus {
        match message {
            QuorumMsg::Request(m) => {
                dscale::send(
                    from,
                    QuorumMsg::Response(Response {
                        cmd_id: m.cmd_id,
                        deps: Arc::clone(&m.deps),
                    }),
                );
                QuorumStatus::DoNothing
            }
            QuorumMsg::Response(m) => {
                let decision = self
                    .instances
                    .get_mut(&m.cmd_id)
                    .is_some_and(|instance| instance.accept(from, &m.deps));
                if decision {
                    let deps = Arc::clone(&self.instances[&m.cmd_id].deps);
                    self.instances.remove(&m.cmd_id);
                    QuorumStatus::Decided(m.cmd_id, deps)
                } else {
                    QuorumStatus::DoNothing
                }
            }
        }
    }

    fn replicas(&self, cmd_id: CmdId) -> Vec<dscale::Pid> {
        if !matches!(&self.protocol_type, BLSMRProtocol::ThreeJane) {
            return dscale::list_pool(POOL_BLSMR);
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decides_after_identical_responses_from_every_replica() {
        let deps: Arc<[CmdId]> = [CmdId { pid: 0, id: 1 }].into();
        let mut instance = Instance::new(Arc::clone(&deps), [1, 2].into());

        assert!(!instance.accept(1, &deps));
        assert!(!instance.accept(1, &deps));
        assert!(!instance.accept(3, &deps));
        assert!(instance.accept(2, &deps));
    }
}
