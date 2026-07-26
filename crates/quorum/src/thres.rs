use dscale::rand::{Rng, seq::IndexedRandom};

#[derive(Clone)]
pub struct QuorumSystem {
    pids: Vec<dscale::Pid>,
    quorum_buffer: Vec<dscale::Pid>,
    f: usize,
    m: usize,
}

impl QuorumSystem {
    pub(super) fn new(pids: Vec<dscale::Pid>, f: usize, m: usize) -> Self {
        let size = pids.len();
        Self {
            pids,
            quorum_buffer: Vec::with_capacity(size),
            f,
            m,
        }
    }
}

impl QuorumSystem {
    pub(super) fn size(&self) -> usize {
        self.m * ((self.pids.len() - 1) / self.f) + self.m - 1
    }
    pub(super) fn choose_random_quorum(&mut self, rng: &mut impl Rng) -> &[dscale::Pid] {
        let size = self.size();
        self.quorum_buffer.clear();
        self.quorum_buffer
            .extend(self.pids.sample(rng, size).copied());
        &self.quorum_buffer
    }

    pub(super) fn is_quorum(&self, pids: impl Iterator<Item = dscale::Pid>) -> bool {
        pids.count() >= self.size()
    }
}
