use std::hint;

use dscale::{
    Jiffies,
    rand::{SeedableRng, rngs::SmallRng},
    services::kv,
};

use crate::{
    KEY_SUBMIT_CMD_INTERVAL,
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
            submit_interval: kv::get::<Jiffies>(KEY_SUBMIT_CMD_INTERVAL),
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
            match self.dds.on_message(from, announce_message) {
                AnnounceStatus::DoNothing => {}
                AnnounceStatus::QuorumReady(quorum) => {
                    if self.dds.is_quorum(quorum.iter().map(|res| res.id.pid)) {
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
    fn on_timer(&mut self, id: dscale::TimerId) {
        if self.dds.is_my_timer(id) {
            self.dds.on_timer(id);
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
        self.current_submit_timer_id = dscale::schedule_timer_after(self.submit_interval);
    }
}
