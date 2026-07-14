use std::hint;

use client::CmdId;
use dscale::{
    Jiffies,
    rand::{SeedableRng, distr::Uniform, prelude::Distribution, rngs::SmallRng},
    services::kv,
};

use crate::{
    KEY_KEY_COUNT, KEY_SUBMIT_INTERVAL, POOL_BLSMR,
    dds::{self, Announce, AnnounceStatus, Commit, DDS},
    log,
    pbft::{Pbft, PbftMsg, PbftStatus},
};

pub struct BLSMR {
    rng: SmallRng,
    submit_interval: dscale::Jiffies,
    key_count: usize,
    current_submit_timer_id: dscale::TimerId,
    dds: DDS,
    pbft: Pbft,
}

impl Default for BLSMR {
    fn default() -> Self {
        Self {
            rng: SmallRng::seed_from_u64(kv::seed()),
            submit_interval: kv::get::<Jiffies>(KEY_SUBMIT_INTERVAL),
            key_count: kv::get::<usize>(KEY_KEY_COUNT),
            current_submit_timer_id: 0,
            dds: DDS::default(),
            pbft: Pbft::default(),
        }
    }
}

impl dscale::Process for BLSMR {
    fn on_start(&mut self) {
        self.sched_submit();
    }
    fn on_message(&mut self, from: dscale::Pid, message: dscale::MessagePtr) {
        if let Some(announce_message) = message.try_as_type::<Announce>() {
            let status = self.dds.on_message(from, announce_message);
            self.handle_announce_status(status);
        } else if let Some(pbft_message) = message.try_as_type::<PbftMsg>() {
            let status = self.pbft.on_message(from, pbft_message);
            self.handle_pbft_status(status);
        }
    }
    fn on_timer(&mut self, id: dscale::TimerId) {
        if self.dds.is_my_timer(id) {
            let status = self.dds.on_timer(id);
            self.handle_announce_status(status);
        } else if id == self.current_submit_timer_id {
            let cmd = client::create_cmd(&mut self.rng, self.key_count);
            self.dds.announce(cmd);
            self.sched_submit();
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
                log::record_conflict(!quorum.allow_fastpath);
                if quorum.allow_fastpath {
                    dscale::dscale_debug!("took fast path");
                    self.decide(cmd_id, deps);
                } else {
                    dscale::dscale_debug!("took slow path");
                    self.pbft.propose(cmd_id, deps);
                }
            }
        }
    }

    fn handle_pbft_status(&mut self, status: PbftStatus) {
        if let PbftStatus::Decided(cmd_id, deps) = status {
            if cmd_id.pid == dscale::pid() {
                self.decide(cmd_id, deps);
            }
        }
    }

    fn decide(&mut self, cmd_id: CmdId, deps: Vec<CmdId>) {
        dscale::broadcast_within_pool(POOL_BLSMR, Announce::Commit(Commit { cmd_id, deps }));
    }
}
