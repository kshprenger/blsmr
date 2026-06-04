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
        self.m * (self.pids.len() / self.f) - 1
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

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    #[test]
    fn size_follows_f_and_m() {
        let qs = QuorumSystem::new((0..9).collect(), 3, 2);
        assert_eq!(qs.size(), 5); // 2 * (9 / 3) - 1
    }

    #[test]
    fn choose_random_quorum_clears_previous_selection() {
        let mut qs = QuorumSystem::new((0..9).collect(), 3, 2);
        let mut rng = StdRng::seed_from_u64(42);

        let first_len = qs.choose_random_quorum(&mut rng).len();
        let second_len = qs.choose_random_quorum(&mut rng).len();

        assert_eq!(first_len, qs.size());
        assert_eq!(second_len, qs.size());
    }

    #[test]
    fn choose_random_quorum_has_no_duplicates_and_is_subset() {
        let pids: Vec<dscale::Pid> = (0..9).collect();
        let mut qs = QuorumSystem::new(pids.clone(), 3, 2);
        let mut rng = StdRng::seed_from_u64(7);

        let quorum = qs.choose_random_quorum(&mut rng).to_vec();

        assert!(quorum.iter().all(|pid| pids.contains(pid)));
        let unique: std::collections::HashSet<_> = quorum.iter().collect();
        assert_eq!(unique.len(), quorum.len());
    }

    #[test]
    fn is_quorum_accepts_generated_quorum() {
        let mut qs = QuorumSystem::new((0..9).collect(), 3, 2);
        let mut rng = StdRng::seed_from_u64(11);

        let quorum = qs.choose_random_quorum(&mut rng).to_vec();

        assert!(qs.is_quorum(quorum.iter().copied()));
    }

    #[test]
    fn is_quorum_rejects_too_small_set() {
        let qs = QuorumSystem::new((0..9).collect(), 3, 2);
        let too_small: Vec<dscale::Pid> = (0..qs.size() - 1).collect();

        assert!(!qs.is_quorum(too_small.iter().copied()));
    }
}
