pub mod dds;
pub mod log;
mod pbft;
pub mod process;

pub const POOL_BLSMR: &str = "blsmr";
pub const KEY_SUBMIT_INTERVAL: &str = "submit_interval";
pub const KEY_QUORUM_SYSTEM: &str = "quorum_system";
pub const KEY_ANNOUNCE_TIMEOUT: &str = "announce_timeout";
pub const KEY_PROTOCOL_TYPE: &str = "protocol_type";
pub const KEY_AVG_COMMIT_LATENCY: &str = "avg_commit_latency";
pub const KEY_CONFLICT_RATE: &str = "conflict_rate";
pub const KEY_KEY_COUNT: &str = "key_count";

#[derive(Clone)]
pub enum BLSMRProtocol {
    EPaxos,
    Wintermute,
    ThreeJane,
}
