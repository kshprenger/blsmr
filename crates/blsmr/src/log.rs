use std::sync::Arc;

use client::{CmdId, Command};
use dscale::{Jiffies, services::kv};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{KEY_AVG_COMMIT_LATENCY, KEY_COMMIT_LATENCIES, KEY_CONFLICT_RATE};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Pending,
    Commit,
    Stable,
}

pub struct ConflictTracker {
    replica_count: usize,
    arrivals: FxHashMap<CmdId, Jiffies>,
    committers: FxHashMap<CmdId, FxHashSet<dscale::Pid>>,
    completed: Vec<(Jiffies, Jiffies)>,
}

impl ConflictTracker {
    pub fn new(replica_count: usize) -> Self {
        Self {
            replica_count,
            arrivals: FxHashMap::default(),
            committers: FxHashMap::default(),
            completed: Vec::new(),
        }
    }

    fn record_arrival(&mut self, cmd_id: CmdId) {
        self.arrivals.entry(cmd_id).or_insert_with(dscale::now);
    }

    fn record_commit(&mut self, cmd_id: CmdId) {
        let committers = self.committers.entry(cmd_id).or_default();
        if committers.insert(dscale::pid()) && committers.len() == self.replica_count {
            let arrival = self
                .arrivals
                .remove(&cmd_id)
                .expect("commit without arrival");
            self.committers.remove(&cmd_id);
            self.completed.push((arrival, dscale::now()));
        }
    }

    fn percentage(&self) -> f64 {
        let pair_count = self.completed.len().saturating_sub(1) * self.completed.len() / 2;
        if pair_count == 0 {
            return 0.0;
        }
        let mut commits: Vec<Jiffies> = self.completed.iter().map(|(_, commit)| *commit).collect();
        commits.sort_unstable();
        let happens_before_pairs: usize = self
            .completed
            .iter()
            .map(|(arrival, _)| commits.partition_point(|commit| commit < arrival))
            .sum();
        100.0 * (pair_count - happens_before_pairs) as f64 / pair_count as f64
    }
}

struct Entry {
    cmd: Option<Command>,
    submitted_at: dscale::Jiffies,
    deps: Option<Arc<[CmdId]>>,
    phase: Phase,
    executed: bool,
}

impl Entry {
    fn deps(&self) -> &[CmdId] {
        self.deps.as_deref().unwrap_or_default()
    }
}

#[derive(Default)]
pub struct CmdLog {
    entries: FxHashMap<CmdId, Entry>,
    // CmdId of every command sharing a given key, so `submit` doesn't have to scan the whole log.
    by_key: FxHashMap<usize, Vec<CmdId>>,
    // Commit-phase commands not yet promoted to Stable, so `promote_stable` only rescans
    // candidates instead of the whole (ever-growing) log on every commit.
    pending_commit: FxHashSet<CmdId>,
    stable: FxHashSet<CmdId>,
    #[cfg(test)]
    execution_order: Vec<CmdId>,
}

impl CmdLog {
    pub fn record_creation(&mut self, cmd_id: CmdId) {
        self.record_creation_at(cmd_id, dscale::now());
    }

    fn record_creation_at(&mut self, cmd_id: CmdId, submitted_at: dscale::Jiffies) {
        self.entries.entry(cmd_id).or_insert_with(|| Entry {
            cmd: None,
            submitted_at,
            deps: None,
            phase: Phase::Pending,
            executed: false,
        });
    }

    pub fn submit(&mut self, cmd: Command) -> Vec<CmdId> {
        if self.stable.contains(&cmd.id) {
            return Vec::new();
        }
        let conflicts = self
            .by_key
            .get(&cmd.key)
            .into_iter()
            .flatten()
            .filter_map(|id| self.entries.get(id))
            .filter(|entry| entry.phase != Phase::Stable)
            .filter_map(|entry| entry.cmd.map(|cmd| cmd.id))
            .collect();

        let entry = self.entries.entry(cmd.id).or_insert_with(|| Entry {
            cmd: None,
            submitted_at: dscale::now(),
            deps: None,
            phase: Phase::Pending,
            executed: false,
        });
        if entry.cmd.is_none() {
            entry.cmd = Some(cmd);
            self.by_key.entry(cmd.key).or_default().push(cmd.id);
        }

        conflicts
    }

    pub fn commit(&mut self, cmd_id: CmdId, deps: Arc<[CmdId]>) {
        if self.stable.contains(&cmd_id) {
            return;
        }
        let entry = self.entries.entry(cmd_id).or_insert_with(|| Entry {
            cmd: None,
            submitted_at: dscale::now(),
            deps: None,
            phase: Phase::Pending,
            executed: false,
        });
        entry.deps = (!deps.is_empty()).then_some(deps);
        entry.phase = Phase::Commit;
        self.pending_commit.insert(cmd_id);
        self.promote_stable();
    }

    pub fn record_decision_latency(&self, cmd_id: CmdId) {
        let submitted_at = self.entries[&cmd_id].submitted_at;
        kv::modify::<Vec<dscale::Jiffies>>(KEY_COMMIT_LATENCIES, |latencies| {
            latencies.push(dscale::now() - submitted_at);
        });
    }

    #[cfg(test)]
    pub fn executed_order(&self) -> &[CmdId] {
        &self.execution_order
    }

    fn promote_stable(&mut self) {
        let newly_stable: Vec<CmdId> = self
            .pending_commit
            .iter()
            .filter(|id| self.is_deps_closure_committed(self.entries[id].deps()))
            .copied()
            .collect();

        for id in newly_stable {
            self.pending_commit.remove(&id);
            let entry = self.entries.get_mut(&id).expect("entry must exist");
            entry.phase = Phase::Stable;
            let latency = dscale::now() - entry.submitted_at;
            let key = entry.cmd.as_ref().map(|cmd| cmd.key);
            record_latency(latency);
            self.try_execute(id);
            if let Some(bucket) = key.and_then(|key| self.by_key.get_mut(&key)) {
                bucket.retain(|&bucketed| bucketed != id);
            }
            self.entries.remove(&id);
            self.stable.insert(id);
        }
    }

    fn is_deps_closure_committed(&self, deps: &[CmdId]) -> bool {
        let mut stack: Vec<CmdId> = deps.to_vec();
        let mut seen = FxHashSet::default();
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            if self.stable.contains(&id) {
                continue;
            }
            match self.entries.get(&id) {
                Some(entry) if matches!(entry.phase, Phase::Commit | Phase::Stable) => {
                    stack.extend(entry.deps().iter().copied());
                }
                _ => return false,
            }
        }
        true
    }

    // Executes `root` and every not-yet-executed command reachable through it, dependencies
    // first.
    fn try_execute(&mut self, root: CmdId) {
        if self.entries.get(&root).is_none_or(|entry| entry.executed) {
            return;
        }

        let pending = self.collect_pending_closure(root);
        for mut scc in self.tarjan_sccs(&pending) {
            scc.sort();
            for id in scc {
                if let Some(entry) = self.entries.get_mut(&id) {
                    if !entry.executed {
                        entry.executed = true;
                        #[cfg(test)]
                        self.execution_order.push(id);
                    }
                }
            }
        }
    }

    fn collect_pending_closure(&self, root: CmdId) -> Vec<CmdId> {
        let mut seen = FxHashSet::default();
        let mut stack = vec![root];
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            if let Some(entry) = self.entries.get(&id) {
                for &dep in entry.deps() {
                    if self.entries.get(&dep).is_some_and(|entry| !entry.executed) {
                        stack.push(dep);
                    }
                }
            }
        }
        let mut nodes: Vec<CmdId> = seen.into_iter().collect();
        nodes.sort();
        nodes
    }

    fn tarjan_sccs(&self, nodes: &[CmdId]) -> Vec<Vec<CmdId>> {
        struct State {
            index: FxHashMap<CmdId, usize>,
            low_link: FxHashMap<CmdId, usize>,
            on_stack: FxHashSet<CmdId>,
            stack: Vec<CmdId>,
            counter: usize,
            sccs: Vec<Vec<CmdId>>,
        }

        fn strongconnect(log: &CmdLog, node_set: &FxHashSet<CmdId>, v: CmdId, s: &mut State) {
            s.index.insert(v, s.counter);
            s.low_link.insert(v, s.counter);
            s.counter += 1;
            s.stack.push(v);
            s.on_stack.insert(v);

            if let Some(entry) = log.entries.get(&v) {
                for &w in entry.deps() {
                    if !node_set.contains(&w) {
                        continue;
                    }
                    if !s.index.contains_key(&w) {
                        strongconnect(log, node_set, w, s);
                        let merged = s.low_link[&v].min(s.low_link[&w]);
                        s.low_link.insert(v, merged);
                    } else if s.on_stack.contains(&w) {
                        let merged = s.low_link[&v].min(s.index[&w]);
                        s.low_link.insert(v, merged);
                    }
                }
            }

            if s.low_link[&v] == s.index[&v] {
                let mut scc = Vec::new();
                loop {
                    let w = s.stack.pop().expect("stack not empty while closing scc");
                    s.on_stack.remove(&w);
                    scc.push(w);
                    if w == v {
                        break;
                    }
                }
                s.sccs.push(scc);
            }
        }

        let node_set: FxHashSet<CmdId> = nodes.iter().copied().collect();
        let mut state = State {
            index: FxHashMap::default(),
            low_link: FxHashMap::default(),
            on_stack: FxHashSet::default(),
            stack: Vec::new(),
            counter: 0,
            sccs: Vec::new(),
        };

        for &node in nodes {
            if !state.index.contains_key(&node) {
                strongconnect(self, &node_set, node, &mut state);
            }
        }

        state.sccs
    }
}

fn record_latency(latency: dscale::Jiffies) {
    kv::modify::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY, |(sum, count)| {
        *sum += latency.0;
        *count += 1;
    });
}

pub fn average_commit_latency() -> f64 {
    let (sum, count) = kv::get::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY);
    if count == 0 {
        0.0
    } else {
        sum as f64 / count as f64
    }
}

pub fn record_arrival(cmd_id: CmdId) {
    kv::modify::<ConflictTracker>(KEY_CONFLICT_RATE, |tracker| {
        tracker.record_arrival(cmd_id);
    });
}

pub fn record_commit(cmd_id: CmdId) {
    kv::modify::<ConflictTracker>(KEY_CONFLICT_RATE, |tracker| {
        tracker.record_commit(cmd_id);
    });
}

pub fn conflict_rate_percentage() -> f64 {
    let mut percentage = 0.0;
    kv::modify::<ConflictTracker>(KEY_CONFLICT_RATE, |tracker| {
        percentage = tracker.percentage();
    });
    percentage
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: usize) -> CmdId {
        CmdId { pid: 0, id: n }
    }

    fn insert_committed(log: &mut CmdLog, cmd_id: CmdId, deps: Vec<CmdId>) {
        log.entries.insert(
            cmd_id,
            Entry {
                cmd: None,
                submitted_at: dscale::Jiffies(0),
                deps: (!deps.is_empty()).then(|| deps.into()),
                phase: Phase::Commit,
                executed: false,
            },
        );
    }

    #[test]
    fn executes_dependency_before_dependent() {
        let mut log = CmdLog::default();
        insert_committed(&mut log, id(1), vec![]);
        insert_committed(&mut log, id(2), vec![id(1)]);

        log.try_execute(id(2));

        assert_eq!(log.executed_order(), &[id(1), id(2)]);
    }

    #[test]
    fn executes_unrelated_command_independently() {
        let mut log = CmdLog::default();
        insert_committed(&mut log, id(1), vec![]);
        insert_committed(&mut log, id(2), vec![]);

        log.try_execute(id(1));

        assert_eq!(log.executed_order(), &[id(1)]);
        assert!(!log.entries[&id(2)].executed);
    }

    #[test]
    fn cycle_executes_as_one_batch_in_canonical_id_order() {
        let mut log = CmdLog::default();
        insert_committed(&mut log, id(2), vec![id(1)]);
        insert_committed(&mut log, id(1), vec![id(2)]);

        log.try_execute(id(2));

        assert_eq!(log.executed_order(), &[id(1), id(2)]);
    }

    #[test]
    fn cycle_discovered_from_either_member_gives_same_batch() {
        let mut log = CmdLog::default();
        insert_committed(&mut log, id(2), vec![id(1)]);
        insert_committed(&mut log, id(1), vec![id(2)]);

        log.try_execute(id(1));

        assert_eq!(log.executed_order(), &[id(1), id(2)]);
    }

    #[test]
    fn dependent_of_a_cycle_executes_after_the_whole_cycle() {
        let mut log = CmdLog::default();
        insert_committed(&mut log, id(3), vec![id(1), id(2)]);
        insert_committed(&mut log, id(1), vec![id(2)]);
        insert_committed(&mut log, id(2), vec![id(1)]);

        log.try_execute(id(3));

        assert_eq!(log.executed_order(), &[id(1), id(2), id(3)]);
    }

    #[test]
    fn already_executed_root_is_a_no_op() {
        let mut log = CmdLog::default();
        insert_committed(&mut log, id(1), vec![]);

        log.try_execute(id(1));
        log.try_execute(id(1));

        assert_eq!(log.executed_order(), &[id(1)]);
    }

    #[test]
    fn stable_tombstone_satisfies_dependencies_without_retaining_entry() {
        let mut log = CmdLog::default();
        log.stable.insert(id(1));
        insert_committed(&mut log, id(2), vec![id(1)]);

        assert!(log.is_deps_closure_committed(&[id(2)]));
        assert!(!log.entries.contains_key(&id(1)));
    }

    #[test]
    fn submission_preserves_local_creation_time() {
        let mut log = CmdLog::default();
        log.record_creation_at(id(1), dscale::Jiffies(7));

        log.submit(Command { id: id(1), key: 3 });

        assert_eq!(log.entries[&id(1)].submitted_at, dscale::Jiffies(7));
    }

    #[test]
    fn conflict_rate_counts_overlapping_pairs() {
        let tracker = ConflictTracker {
            replica_count: 1,
            arrivals: FxHashMap::default(),
            committers: FxHashMap::default(),
            completed: vec![
                (dscale::Jiffies(0), dscale::Jiffies(10)),
                (dscale::Jiffies(5), dscale::Jiffies(8)),
                (dscale::Jiffies(11), dscale::Jiffies(12)),
            ],
        };

        assert_eq!(tracker.percentage(), 100.0 / 3.0);
    }
}
