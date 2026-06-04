use client::{CmdId, Command};
use dscale::{
    Jiffies, dscale_debug, helpers,
    rand::{SeedableRng, rngs::SmallRng},
    services::kv,
};
use rustc_hash::FxHashMap;

use crate::{KEY_ANNOUNCE_TIMEOUT, KEY_QUORUM_SYSTEM, POOL_BLSMR, log::CmdLog};

pub enum AnnounceStatus {
    DoNothing,
    QuorumReady(Vec<Res>),
}

#[derive(Debug)]
pub enum Announce {
    Req(Req),
    Res(Res),
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

impl dscale::Message for Announce {}

pub(crate) struct DDS {
    rng: SmallRng,
    peer_number: usize,
    bqs: quorum::QuorumSystem,
    log: CmdLog,
    pending_announce_quorums: FxHashMap<CmdId, helpers::Quorum<Res>>,
    announce_timeout: dscale::Jiffies,
    announce_force_timers: FxHashMap<dscale::TimerId, CmdId>,
}

impl Default for DDS {
    fn default() -> Self {
        Self {
            rng: SmallRng::seed_from_u64(kv::seed()),
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
        match self.announce_timeout {
            Jiffies(0) => {
                self.pending_announce_quorums
                    .insert(cmd.id, helpers::Quorum::new(self.bqs.size()));
            }
            timeout @ _ => {
                self.pending_announce_quorums
                    .insert(cmd.id, helpers::Quorum::new(self.peer_number));
                self.announce_force_timers
                    .insert(dscale::schedule_timer_after(timeout), cmd.id);
            }
        }
    }

    fn send_announce(&mut self, cmd: client::Command) {
        match self.announce_timeout {
            Jiffies(0) => dscale::broadcast_within_pool(POOL_BLSMR, Announce::Req(Req { cmd })),
            _ => dscale::send_many(
                self.bqs.choose_random_quorum(&mut self.rng), // 3Jane
                Announce::Req(Req { cmd }),
            ),
        }
    }

    pub(super) fn on_message(&mut self, from: dscale::Pid, message: &Announce) -> AnnounceStatus {
        match message {
            Announce::Req(req) => {
                dscale::send(
                    from,
                    Announce::Res(Res {
                        id: req.cmd.id,
                        conflicts: self.log.conflicts(req.cmd.clone()),
                    }),
                );
                AnnounceStatus::DoNothing
            }
            Announce::Res(res) => {
                match self.pending_announce_quorums.get_mut(&res.id) {
                    None => panic!("failed to find pending quorum for cmd"),
                    Some(quorum) => {
                        if let Some(quorum) = quorum.add(res.clone()) {
                            self.pending_announce_quorums.remove(&res.id);
                            dscale_debug!("quorum ready for CmdId: {:?}", res.id);
                            return AnnounceStatus::QuorumReady(quorum);
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
            None => unreachable!("quorum not found"),
            Some(quorum) => {
                if quorum.size() >= self.bqs.size() {
                    let results = self
                        .pending_announce_quorums
                        .remove(&cmd_id)
                        .expect("quorum not found")
                        .force_extract()
                        .expect("quorum was already exhausted");
                    AnnounceStatus::QuorumReady(results)
                } else {
                    dscale::dscale_warn!("quorum was not reached until timeout");
                    AnnounceStatus::DoNothing
                }
            }
        }
    }
    pub(super) fn is_quorum(&self, pids: impl Iterator<Item = dscale::Pid> + Clone) -> bool {
        self.bqs.is_quorum(pids)
    }
}
