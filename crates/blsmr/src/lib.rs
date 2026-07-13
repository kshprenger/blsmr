pub mod dds;
pub mod log;
pub mod process;

pub const POOL_BLSMR: &str = "blsmr";
pub const KEY_SUBMIT_INTERVAL: &str = "submit_interval";
pub const KEY_QUORUM_SYSTEM: &str = "quorum_system";
pub const KEY_ANNOUNCE_TIMEOUT: &str = "announce_timeout";
pub const KEY_PROTOCOL_TYPE: &str = "protocol_type";

#[derive(Clone)]
pub enum BLSMRProtocol {
    EPaxos,
    Wintermute,
    ThreeJane,
}
