use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use dscale::{
    Jiffies, TimerId,
    rand::{Rng, SeedableRng, rngs::SmallRng, seq::SliceRandom},
    services::kv,
};

pub const KEY_REPLAY_TRACE: &str = "client_replay_trace";
pub const KEY_REPLAY_METRICS: &str = "client_replay_metrics";
pub const VISA_CLIENTS_PER_REPLICA: usize = 10;
pub const VISA_TRANSFER_COUNT: usize = 540_000;
pub const VISA_TRANSFERS_PER_SECOND: usize = 1_800;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct CmdId {
    pub pid: dscale::Pid,
    pub id: usize,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum CommandAccess {
    SingleKey(usize),
    Transfer {
        from: usize,
        to: usize,
        amount: usize,
    },
}

impl CommandAccess {
    pub fn keys(&self) -> impl Iterator<Item = usize> {
        let (first, second) = match *self {
            Self::SingleKey(key) => (key, None),
            Self::Transfer { from, to, .. } => (from, (from != to).then_some(to)),
        };
        std::iter::once(first).chain(second)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Command {
    pub id: CmdId,             // unique id
    pub access: CommandAccess, // conflicts
}

pub fn create_cmd(rng: &mut impl Rng, key_count: usize) -> Command {
    use dscale::rand::prelude::IteratorRandom;
    create_cmd_for_key((0..key_count).choose(rng).expect("choose failed"))
}

pub fn create_cmd_for_key(key: usize) -> Command {
    create_command(CommandAccess::SingleKey(key))
}

pub fn create_transfer(from: usize, to: usize, amount: usize) -> Command {
    create_command(CommandAccess::Transfer { from, to, amount })
}

fn create_command(access: CommandAccess) -> Command {
    Command {
        id: CmdId {
            pid: dscale::pid(),
            id: dscale::unique_id(),
        },
        access,
    }
}

impl Command {
    pub fn conflicts_with(&self, other_cmd: &Command) -> bool {
        self.access
            .keys()
            .any(|key| other_cmd.access.keys().any(|other_key| key == other_key))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct TraceRecord {
    pub client_id: usize,
    pub sequence: usize,
    pub at: Jiffies,
    pub from: usize,
    pub to: usize,
    pub amount: usize,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ReplayTrace {
    pub replica_count: usize,
    pub records: Vec<TraceRecord>,
}

#[derive(Debug)]
pub struct ReplayMetrics {
    replicas: Vec<dscale::Pid>,
    submitted: Vec<AtomicUsize>,
}

impl ReplayMetrics {
    pub fn new(replica_count: usize) -> Self {
        assert!(replica_count > 0, "replica count must be positive");
        let client_count = replica_count
            .checked_mul(VISA_CLIENTS_PER_REPLICA)
            .expect("client count overflow");
        let replicas = dscale::list_pool(dscale::GLOBAL_POOL);
        assert_eq!(replicas.len(), replica_count, "replica count mismatch");
        Self {
            replicas,
            submitted: (0..client_count).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    pub fn record_submission(&self, record: &TraceRecord) {
        assert_eq!(
            self.replicas[record.client_id % self.replicas.len()],
            dscale::pid(),
            "wrong replay replica"
        );
        assert_eq!(record.at, dscale::now(), "wrong submission time");
        let previous = self.submitted[record.client_id].fetch_add(1, Ordering::Relaxed);
        assert_eq!(record.sequence, previous, "wrong client sequence");
    }

    pub fn snapshot(&self) -> Vec<usize> {
        self.submitted
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect()
    }
}

#[derive(Debug)]
pub struct ReplayState {
    trace: Arc<ReplayTrace>,
    local_indexes: Vec<usize>,
    cursor: usize,
    timer: Option<TimerId>,
    started: bool,
    metrics: Arc<ReplayMetrics>,
}

impl ReplayState {
    pub fn from_kv() -> Option<Self> {
        let trace = kv::get::<Option<Arc<ReplayTrace>>>(KEY_REPLAY_TRACE)?;
        let metrics = kv::get::<Arc<ReplayMetrics>>(KEY_REPLAY_METRICS);
        let replicas = dscale::list_pool(dscale::GLOBAL_POOL);
        assert_eq!(
            replicas.len(),
            trace.replica_count,
            "replay replica count mismatch"
        );
        let ordinal = replicas
            .iter()
            .position(|&pid| pid == dscale::pid())
            .expect("current process is not in the global pool");
        let mut local_indexes = Vec::new();
        let mut previous = None;
        for (index, record) in trace.records.iter().enumerate() {
            assert!(
                record.client_id < trace.replica_count * VISA_CLIENTS_PER_REPLICA,
                "invalid client ID"
            );
            let order = (record.at, record.client_id, record.sequence);
            assert!(
                previous.is_none_or(|prior| prior <= order),
                "unordered replay trace"
            );
            previous = Some(order);
            if record.client_id % replicas.len() == ordinal {
                local_indexes.push(index);
            }
        }
        Some(Self {
            trace,
            local_indexes,
            cursor: 0,
            timer: None,
            started: false,
            metrics,
        })
    }

    pub fn start(&mut self) {
        assert!(!self.started, "replay already started");
        self.started = true;
        self.schedule_next();
    }

    pub fn is_timer(&self, id: TimerId) -> bool {
        self.timer == Some(id)
    }

    pub fn take_due(&mut self, id: TimerId) -> Vec<TraceRecord> {
        assert!(self.is_timer(id), "unexpected replay timer");
        self.timer = None;
        let now = dscale::now();
        let Some(&first) = self.local_indexes.get(self.cursor) else {
            return Vec::new();
        };
        if self.trace.records[first].at > now {
            self.schedule_next();
            return Vec::new();
        }
        assert_eq!(
            self.trace.records[first].at, now,
            "replay timer fired at wrong time"
        );
        let mut due = Vec::new();
        while let Some(&index) = self.local_indexes.get(self.cursor) {
            let record = self.trace.records[index];
            assert!(record.at >= now, "overdue replay record");
            if record.at != now {
                break;
            }
            due.push(record);
            self.cursor += 1;
        }
        self.schedule_next();
        due
    }

    pub fn record_submission(&self, record: &TraceRecord) {
        self.metrics.record_submission(record);
    }

    fn schedule_next(&mut self) {
        assert!(self.timer.is_none(), "replay timer already scheduled");
        if let Some(&index) = self.local_indexes.get(self.cursor) {
            let target = self.trace.records[index].at;
            let now = dscale::now();
            assert!(target >= now, "overdue replay record");
            self.timer = Some(dscale::schedule_timer_after(target - now));
        }
    }
}

pub fn generate_visa_trace(account_count: usize, seed: u64, replica_count: usize) -> ReplayTrace {
    assert!(
        account_count >= 2 && account_count % 2 == 0,
        "account count must be even and at least 2"
    );
    assert!(replica_count > 0, "replica count must be positive");
    let client_count = replica_count
        .checked_mul(VISA_CLIENTS_PER_REPLICA)
        .expect("client count overflow");
    assert_eq!(
        VISA_TRANSFER_COUNT % client_count,
        0,
        "unequal client workloads"
    );
    let mut records = Vec::with_capacity(VISA_TRANSFER_COUNT);
    let mut accounts: Vec<_> = (0..account_count).collect();
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut next_pair = account_count;

    for index in 0..VISA_TRANSFER_COUNT {
        let at_us = u64::try_from(index)
            .expect("index does not fit in u64")
            .checked_mul(1_000_000)
            .and_then(|value| value.checked_add((VISA_TRANSFERS_PER_SECOND / 2) as u64))
            .expect("timestamp overflow")
            / VISA_TRANSFERS_PER_SECOND as u64;
        if next_pair == account_count {
            accounts.shuffle(&mut rng);
            next_pair = 0;
        }
        let from = accounts[next_pair];
        let to = accounts[next_pair + 1];
        assert_ne!(from, to);
        records.push(TraceRecord {
            client_id: index % client_count,
            sequence: index / client_count,
            at: Jiffies(usize::try_from(at_us).expect("timestamp does not fit in usize")),
            from,
            to,
            amount: 1,
        });
        next_pair += 2;
    }
    ReplayTrace {
        replica_count,
        records,
    }
}
