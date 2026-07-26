use std::{hint, sync::Arc};

use client::CmdId;
use dscale::{
    Jiffies,
    rand::{SeedableRng, distr::Uniform, prelude::Distribution, rngs::SmallRng},
    services::kv,
};

use crate::{
    KEY_KEY_COUNT, KEY_SUBMIT_INTERVAL, KEY_SUBMIT_LIMIT, KEY_TRACK_CONFLICT_RATE, POOL_BLSMR,
    dds::{self, Announce, AnnounceStatus, Commit, DDS},
    log,
    quorum::{Quorum, QuorumMsg, QuorumStatus},
};

pub struct BLSMR {
    rng: SmallRng,
    submit_interval: dscale::Jiffies,
    submit_limit: usize,
    submitted: usize,
    key_count: usize,
    current_submit_timer_id: dscale::TimerId,
    track_conflict_rate: bool,
    dds: DDS,
    quorum: Quorum,
}

impl Default for BLSMR {
    fn default() -> Self {
        Self {
            rng: SmallRng::seed_from_u64(kv::seed()),
            submit_interval: kv::get::<Jiffies>(KEY_SUBMIT_INTERVAL),
            submit_limit: kv::get(KEY_SUBMIT_LIMIT),
            submitted: 0,
            key_count: kv::get::<usize>(KEY_KEY_COUNT),
            current_submit_timer_id: 0,
            track_conflict_rate: kv::get(KEY_TRACK_CONFLICT_RATE),
            dds: DDS::default(),
            quorum: Quorum::default(),
        }
    }
}

impl dscale::Process for BLSMR {
    fn on_start(&mut self) {
        if self.submit_limit != 0 {
            self.sched_submit();
        }
    }
    fn on_message(&mut self, from: dscale::Pid, message: dscale::MessagePtr) {
        if let Some(announce_message) = message.try_as_type::<Announce>() {
            if self.track_conflict_rate {
                match announce_message {
                    Announce::Req(req) => log::record_arrival(req.cmd.id),
                    Announce::Commit(commit) => log::record_arrival(commit.cmd_id),
                    Announce::Res(_) => {}
                }
            }
            let status = self.dds.on_message(from, announce_message);
            if self.track_conflict_rate
                && let Announce::Commit(commit) = announce_message
            {
                log::record_commit(commit.cmd_id);
            }
            self.handle_announce_status(status);
        } else if let Some(quorum_message) = message.try_as_type::<QuorumMsg>() {
            let status = self.quorum.on_message(from, quorum_message);
            self.handle_quorum_status(status);
        }
    }
    fn on_timer(&mut self, id: dscale::TimerId) {
        if self.dds.is_my_timer(id) {
            let status = self.dds.on_timer(id);
            self.handle_announce_status(status);
        } else if id == self.current_submit_timer_id {
            let cmd = client::create_cmd(&mut self.rng, self.key_count);
            if self.track_conflict_rate {
                log::record_arrival(cmd.id);
            }
            self.dds.announce(cmd);
            self.submitted += 1;
            if self.submitted < self.submit_limit {
                self.sched_submit();
            }
        } else {
            hint::cold_path();
            unreachable!("unknown timer")
        }
    }
}

impl BLSMR {
    fn sched_submit(&mut self) {
        let delay = Uniform::new_inclusive(self.submit_interval.0 / 2, self.submit_interval.0)
            .expect("invalid submit interval")
            .sample(&mut self.rng);
        self.current_submit_timer_id = dscale::schedule_timer_after(Jiffies(delay));
    }

    fn handle_announce_status(&mut self, status: AnnounceStatus) {
        match status {
            AnnounceStatus::DoNothing => {}
            AnnounceStatus::QuorumReady(quorum) => {
                let cmd_id = quorum.quorum[0].id;
                let deps = dds::union_deps(&quorum.quorum);
                if quorum.allow_fastpath {
                    dscale::dscale_debug!("took fast path");
                    self.decide(cmd_id, deps);
                } else {
                    dscale::dscale_debug!("took slow path");
                    self.quorum.propose(cmd_id, deps);
                }
            }
        }
    }

    fn handle_quorum_status(&mut self, status: QuorumStatus) {
        if let QuorumStatus::Decided(cmd_id, deps) = status {
            self.decide(cmd_id, deps);
        }
    }

    fn decide(&mut self, cmd_id: CmdId, deps: Arc<[CmdId]>) {
        self.dds.record_decision_latency(cmd_id);
        dscale::broadcast_within_pool(POOL_BLSMR, Announce::Commit(Commit { cmd_id, deps }));
    }
}
