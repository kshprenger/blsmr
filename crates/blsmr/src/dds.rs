use std::collections::HashSet;

use client::{CmdId, Command};
use dscale::{
    Jiffies, dscale_debug,
    rand::{SeedableRng, rngs::SmallRng},
    services::kv::{self},
};
use rustc_hash::FxHashMap;

use crate::{
    BLSMRProtocol, KEY_ANNOUNCE_TIMEOUT, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, POOL_BLSMR,
    log::CmdLog,
};

pub enum AnnounceStatus {
    DoNothing,
    QuorumReady(QuorumReady),
}

pub struct QuorumReady {
    pub quorum: Vec<Res>,
    pub allow_fastpath: bool,
}

#[derive(Debug)]
pub enum Announce {
    Req(Req),
    Res(Res),
    Commit(Commit),
}

#[derive(Debug)]
pub struct Req {
    pub cmd: client::Command,
}

#[derive(Debug, Clone)]
pub struct Res {
    pub id: client::CmdId,
    pub conflicts: Vec<Command>,
}

#[derive(Debug)]
pub struct Commit {
    pub cmd_id: CmdId,
    pub deps: Vec<CmdId>,
}

impl dscale::Message for Announce {}

pub(crate) struct DDS {
    rng: SmallRng,
    protocol_type: BLSMRProtocol,
    peer_number: usize,
    bqs: quorum::QuorumSystem,
    log: CmdLog,
    pending_announce_quorums: FxHashMap<CmdId, Vec<Res>>,
    announce_timeout: dscale::Jiffies,
    announce_force_timers: FxHashMap<dscale::TimerId, CmdId>,
}

impl Default for DDS {
    fn default() -> Self {
        Self {
            rng: SmallRng::seed_from_u64(kv::seed()),
            protocol_type: kv::get::<BLSMRProtocol>(KEY_PROTOCOL_TYPE),
            peer_number: dscale::list_pool(POOL_BLSMR).len(),
            bqs: kv::get::<quorum::QuorumSystem>(KEY_QUORUM_SYSTEM),
            log: CmdLog::default(),
            pending_announce_quorums: FxHashMap::default(),
            announce_timeout: kv::get::<dscale::Jiffies>(KEY_ANNOUNCE_TIMEOUT),
            announce_force_timers: FxHashMap::default(),
        }
    }
}

impl DDS {
    pub(super) fn announce(&mut self, cmd: client::Command) {
        self.prepare_announce(&cmd);
        self.send_announce(cmd);
    }

    fn prepare_announce(&mut self, cmd: &client::Command) {
        match self.protocol_type {
            BLSMRProtocol::Wintermute => {
                debug_assert!(self.announce_timeout != Jiffies(0));
                self.announce_force_timers
                    .insert(dscale::schedule_timer_after(self.announce_timeout), cmd.id);
            }
            _ => {}
        }
        self.pending_announce_quorums.insert(cmd.id, Vec::new());
    }

    fn send_announce(&mut self, cmd: client::Command) {
        match self.protocol_type {
            BLSMRProtocol::ThreeJane => dscale::send_many(
                self.bqs.choose_random_quorum(&mut self.rng),
                Announce::Req(Req { cmd }),
            ),
            _ => dscale::broadcast_within_pool(POOL_BLSMR, Announce::Req(Req { cmd })),
        }
    }

    pub(super) fn on_message(&mut self, from: dscale::Pid, message: &Announce) -> AnnounceStatus {
        match message {
            Announce::Req(req) => {
                dscale::send(
                    from,
                    Announce::Res(Res {
                        id: req.cmd.id,
                        conflicts: self.log.submit(req.cmd.clone()),
                    }),
                );
                AnnounceStatus::DoNothing
            }
            Announce::Commit(commit) => {
                self.log.commit(commit.cmd_id, commit.deps.clone());
                AnnounceStatus::DoNothing
            }
            Announce::Res(res) => {
                match self.pending_announce_quorums.get_mut(&res.id) {
                    // Already decided via the message path or the force timeout; a straggler's reply.
                    None => {}
                    Some(quorum) => {
                        quorum.push(res.clone());
                        let ready = match self.protocol_type {
                            BLSMRProtocol::Wintermute => quorum.len() == self.peer_number,
                            _ => self.bqs.is_quorum(quorum.iter().map(|res| res.id.pid)),
                        };
                        if ready {
                            let quorum = self
                                .pending_announce_quorums
                                .remove(&res.id)
                                .expect("no quorum");
                            dscale_debug!("quorum ready for CmdId: {:?}", res.id);
                            let allow_fastpath = self.allow_fastpath(&quorum);
                            return AnnounceStatus::QuorumReady(QuorumReady {
                                quorum,
                                allow_fastpath,
                            });
                        }
                    }
                }
                AnnounceStatus::DoNothing
            }
        }
    }

    pub(super) fn is_my_timer(&self, timer_id: dscale::TimerId) -> bool {
        self.announce_force_timers.contains_key(&timer_id)
    }

    pub(super) fn on_timer(&mut self, id: dscale::TimerId) -> AnnounceStatus {
        let cmd_id = self
            .announce_force_timers
            .remove(&id)
            .expect("wrong timer id");
        match self.pending_announce_quorums.get(&cmd_id) {
            // Already decided via the message path before this force timeout fired.
            None => AnnounceStatus::DoNothing,
            Some(quorum) => {
                if self.is_quorum(quorum) {
                    let allow_fastpath = self.allow_fastpath(quorum);
                    let results = self
                        .pending_announce_quorums
                        .remove(&cmd_id)
                        .expect("quorum not found");
                    AnnounceStatus::QuorumReady(QuorumReady {
                        quorum: results,
                        allow_fastpath,
                    })
                } else {
                    dscale::dscale_warn!("quorum was not reached until timeout");
                    AnnounceStatus::DoNothing
                }
            }
        }
    }
    pub(super) fn is_quorum(&self, res: &Vec<Res>) -> bool {
        self.bqs.is_quorum(res.iter().map(|res| res.id.pid))
    }

    fn allow_fastpath(&self, quorum: &[Res]) -> bool {
        if quorum.len() != self.peer_number {
            return false;
        }
        let first = &quorum[0].conflicts;
        let first_ids: HashSet<CmdId> = first.iter().map(|cmd| cmd.id).collect();
        quorum[1..].iter().all(|res| {
            res.conflicts.len() == first.len()
                && res.conflicts.iter().all(|cmd| first_ids.contains(&cmd.id))
        })
    }
}

pub(super) fn union_deps(quorum: &[Res]) -> Vec<CmdId> {
    let mut deps: Vec<CmdId> = quorum
        .iter()
        .flat_map(|res| res.conflicts.iter().map(|cmd| cmd.id))
        .collect();
    deps.sort();
    deps.dedup();
    deps
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dds(peer_number: usize) -> DDS {
        DDS {
            rng: SmallRng::seed_from_u64(0),
            protocol_type: BLSMRProtocol::EPaxos,
            peer_number,
            bqs: quorum::QuorumSystem::new_dissemination(vec![0, 1, 2]),
            log: CmdLog::default(),
            pending_announce_quorums: FxHashMap::default(),
            announce_timeout: Jiffies(0),
            announce_force_timers: FxHashMap::default(),
        }
    }

    fn res_with_conflicts(pid: dscale::Pid, conflict_ids: &[usize]) -> Res {
        Res {
            id: CmdId { pid, id: 0 },
            conflicts: conflict_ids
                .iter()
                .map(|&id| Command {
                    id: CmdId { pid: 0, id },
                    key: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn allow_fastpath_true_when_full_quorum_agrees_on_conflicts() {
        let dds = make_dds(3);
        let quorum = vec![
            res_with_conflicts(0, &[1, 2]),
            res_with_conflicts(1, &[1, 2]),
            res_with_conflicts(2, &[1, 2]),
        ];

        assert!(dds.allow_fastpath(&quorum));
    }

    #[test]
    fn allow_fastpath_true_regardless_of_conflict_order() {
        let dds = make_dds(2);
        let quorum = vec![
            res_with_conflicts(0, &[1, 2, 3]),
            res_with_conflicts(1, &[3, 1, 2]),
        ];

        assert!(dds.allow_fastpath(&quorum));
    }

    #[test]
    fn allow_fastpath_true_when_no_conflicts_reported() {
        let dds = make_dds(2);
        let quorum = vec![res_with_conflicts(0, &[]), res_with_conflicts(1, &[])];

        assert!(dds.allow_fastpath(&quorum));
    }

    #[test]
    fn allow_fastpath_false_when_quorum_smaller_than_peer_number() {
        let dds = make_dds(3);
        let quorum = vec![res_with_conflicts(0, &[1]), res_with_conflicts(1, &[1])];

        assert!(!dds.allow_fastpath(&quorum));
    }

    #[test]
    fn allow_fastpath_false_when_conflicts_differ() {
        let dds = make_dds(2);
        let quorum = vec![
            res_with_conflicts(0, &[1, 2]),
            res_with_conflicts(1, &[1, 3]),
        ];

        assert!(!dds.allow_fastpath(&quorum));
    }

    #[test]
    fn allow_fastpath_false_when_conflict_counts_differ() {
        let dds = make_dds(2);
        let quorum = vec![
            res_with_conflicts(0, &[1, 2]),
            res_with_conflicts(1, &[1, 2, 2]),
        ];

        assert!(!dds.allow_fastpath(&quorum));
    }
}
