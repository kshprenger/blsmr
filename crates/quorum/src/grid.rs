use dscale::rand::{Rng, prelude::SliceRandom};

#[derive(Clone)]
pub struct QuorumSystem {
    flatten_grid: Vec<dscale::Pid>,
    n: usize,
    quorum_buffer: Vec<dscale::Pid>,
    x: usize, // how many rows and columns to take
    indices: Vec<usize>,
    consensus_committee_size: usize,
}

impl QuorumSystem {
    pub(super) fn new(pids: Vec<dscale::Pid>) -> Self {
        let n = pids.len().isqrt();
        let faults = ((n - 2) / 3).max(1);
        Self::with_faults(pids, faults)
    }

    pub(super) fn with_faults(pids: Vec<dscale::Pid>, faults: usize) -> Self {
        let size = pids.len();
        let n = size.isqrt();
        assert_eq!(n * n, size);
        let f = (n as f64 - 2.0) / 3.0;
        let x = ((3.0 * f) / 2.0 + 1.0).sqrt().ceil() as usize;
        Self {
            flatten_grid: pids,
            n,
            quorum_buffer: Vec::with_capacity(2 * x * n),
            x,
            indices: (0..n).collect(),
            consensus_committee_size: (3 * faults + 1).min(size),
        }
    }
}

impl QuorumSystem {
    fn idx(&self, row: usize, col: usize) -> usize {
        row * self.n + col
    }
    pub(super) fn size(&self) -> usize {
        self.x * self.flatten_grid.len().isqrt()
    }
    pub(super) fn consensus_committee_size(&self) -> usize {
        self.consensus_committee_size
    }

    pub(super) fn choose_random_quorum(&mut self, rng: &mut impl Rng) -> &[dscale::Pid] {
        self.quorum_buffer.clear();

        self.indices.shuffle(rng);
        let row_indices: Vec<usize> = self.indices.iter().copied().take(self.x).collect();

        self.indices.shuffle(rng);
        let col_indices: Vec<usize> = self.indices.iter().copied().take(self.x).collect();

        for r in &row_indices {
            for c in 0..self.n {
                self.quorum_buffer.push(self.flatten_grid[self.idx(*r, c)]);
            }
        }

        for c in &col_indices {
            for r in 0..self.n {
                if !row_indices.contains(&r) {
                    self.quorum_buffer.push(self.flatten_grid[self.idx(r, *c)]);
                }
            }
        }

        &self.quorum_buffer
    }

    pub(super) fn is_quorum(&self, pids: impl Iterator<Item = dscale::Pid> + Clone) -> bool {
        let contains = |pid| pids.clone().any(|member| member == pid);

        let full_rows = (0..self.n)
            .filter(|&r| (0..self.n).all(|c| contains(self.flatten_grid[self.idx(r, c)])))
            .count();
        let full_cols = (0..self.n)
            .filter(|&c| (0..self.n).all(|r| contains(self.flatten_grid[self.idx(r, c)])))
            .count();

        full_rows >= self.x && full_cols >= self.x
    }
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    #[test]
    fn size_and_shape_for_5x5_grid() {
        let qs = QuorumSystem::new((0..25).collect());
        assert_eq!(qs.n, 5);
        assert_eq!(qs.x, 2);
        assert_eq!(qs.size(), 10); // x * sqrt(len) = 2 * 5
    }

    #[test]
    fn consensus_committee_size_for_8x8_grid() {
        let qs = QuorumSystem::new((0..64).collect());

        assert_eq!(qs.consensus_committee_size(), 7);
    }

    #[test]
    fn consensus_committee_uses_fixed_fault_count() {
        let qs = QuorumSystem::with_faults((0..64).collect(), 1);

        assert_eq!(qs.consensus_committee_size(), 4);
    }

    #[test]
    #[should_panic]
    fn new_panics_on_non_square_pid_count() {
        QuorumSystem::new((0..10).collect());
    }

    #[test]
    fn choose_random_quorum_covers_full_rows_and_columns() {
        let pids: Vec<dscale::Pid> = (0..25).collect();
        let mut qs = QuorumSystem::new(pids.clone());
        let mut rng = StdRng::seed_from_u64(3);

        let quorum = qs.choose_random_quorum(&mut rng).to_vec();

        // x rows fully included (x * n) + x columns restricted to the
        // remaining rows (x * (n - x)).
        let expected_len = qs.x * qs.n + qs.x * (qs.n - qs.x);
        assert_eq!(quorum.len(), expected_len);
        assert!(quorum.iter().all(|pid| pids.contains(pid)));

        let unique: std::collections::HashSet<_> = quorum.iter().collect();
        assert_eq!(unique.len(), quorum.len(), "quorum must not repeat pids");
    }

    #[test]
    fn is_quorum_accepts_generated_quorum() {
        let mut qs = QuorumSystem::new((0..25).collect());
        let mut rng = StdRng::seed_from_u64(5);

        let quorum = qs.choose_random_quorum(&mut rng).to_vec();

        assert!(qs.is_quorum(quorum.iter().copied()));
    }

    #[test]
    fn is_quorum_accepts_any_x_rows_and_columns() {
        let qs = QuorumSystem::new((0..25).collect());
        let mut hand_picked = Vec::new();
        for r in [0usize, 1] {
            for c in 0..qs.n {
                hand_picked.push(qs.flatten_grid[qs.idx(r, c)]);
            }
        }
        for c in [2usize, 4] {
            for r in 0..qs.n {
                hand_picked.push(qs.flatten_grid[qs.idx(r, c)]);
            }
        }

        assert!(qs.is_quorum(hand_picked.iter().copied()));
    }

    #[test]
    fn is_quorum_rejects_insufficient_rows_or_columns() {
        let qs = QuorumSystem::new((0..25).collect());

        let one_row: Vec<dscale::Pid> = (0..qs.n).map(|c| qs.flatten_grid[qs.idx(0, c)]).collect();
        assert!(!qs.is_quorum(one_row.iter().copied()));
    }

    #[test]
    fn is_quorum_is_upward_closed() {
        let mut qs = QuorumSystem::new((0..25).collect());
        let mut rng = StdRng::seed_from_u64(9);

        let mut superset = qs.choose_random_quorum(&mut rng).to_vec();
        let extra = *qs
            .flatten_grid
            .iter()
            .find(|pid| !superset.contains(pid))
            .unwrap();
        superset.push(extra);

        assert!(qs.is_quorum(superset.iter().copied()));
    }
}
