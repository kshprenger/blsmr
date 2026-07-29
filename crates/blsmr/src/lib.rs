pub mod dds;
pub mod log;
pub mod process;
mod quorum;

pub const POOL_BLSMR: &str = dscale::GLOBAL_POOL;
pub const KEY_SUBMIT_INTERVAL: &str = "submit_interval";
pub const KEY_SUBMIT_LIMIT: &str = "submit_limit";
pub const KEY_QUORUM_SYSTEM: &str = "quorum_system";
pub const KEY_ANNOUNCE_TIMEOUT: &str = "announce_timeout";
pub const KEY_PROTOCOL_TYPE: &str = "protocol_type";
pub const KEY_AVG_COMMIT_LATENCY: &str = "avg_commit_latency";
pub const KEY_COMMIT_LATENCIES: &str = "commit_latencies";
pub const KEY_CONFLICT_RATE: &str = "conflict_rate";
pub const KEY_TRACK_CONFLICT_RATE: &str = "track_conflict_rate";
pub const KEY_KEY_COUNT: &str = "key_count";
pub const KEY_ZIPF_EXPONENT: &str = "zipf_exponent";

#[derive(Clone)]
pub enum BLSMRProtocol {
    EPaxos,
    Wintermute,
    ThreeJane,
}
