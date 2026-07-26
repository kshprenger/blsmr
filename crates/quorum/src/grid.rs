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
