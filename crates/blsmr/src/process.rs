use std::hint;

use dscale::{
    Jiffies,
    rand::{SeedableRng, distr::Uniform, prelude::Distribution, rngs::SmallRng},
    services::kv,
};

use crate::{
    KEY_SUBMIT_INTERVAL,
    dds::{Announce, AnnounceStatus, DDS},
};

pub struct BLSMR {
    rng: SmallRng,
    submit_interval: dscale::Jiffies,
    current_submit_timer_id: dscale::TimerId,
    dds: DDS,
}

impl Default for BLSMR {
    fn default() -> Self {
        Self {
            rng: SmallRng::seed_from_u64(kv::seed()),
            submit_interval: kv::get::<Jiffies>(KEY_SUBMIT_INTERVAL),
            current_submit_timer_id: 0,
            dds: DDS::default(),
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
        }
    }
    fn on_timer(&mut self, id: dscale::TimerId) {
        if self.dds.is_my_timer(id) {
            let status = self.dds.on_timer(id);
            self.handle_announce_status(status);
        } else if id == self.current_submit_timer_id {
            let cmd = client::create_cmd(&mut self.rng);
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

    fn handle_announce_status(&self, status: AnnounceStatus) {
        match status {
            AnnounceStatus::DoNothing => {}
            AnnounceStatus::QuorumReady(quorum) => {
                if quorum.allow_fastpath {
                    dscale::dscale_debug!("took fast path")
                    // Fast path
                } else {
                    dscale::dscale_debug!("took slow path")
                    // Consensus
                }
            }
        }
    }
}
