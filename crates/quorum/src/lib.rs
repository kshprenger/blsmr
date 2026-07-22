use dscale::rand::Rng;

pub mod grid;
mod thres;

#[derive(Clone)]
enum Flavor {
    Threshold(thres::QuorumSystem),
    WitnessGrid(grid::QuorumSystem),
}

#[derive(Clone)]
pub struct QuorumSystem {
    flavor: Flavor,
}

impl QuorumSystem {
    pub fn new_dissemination(pids: Vec<dscale::Pid>) -> Self {
        Self {
            flavor: Flavor::Threshold(thres::QuorumSystem::new(pids, 3, 2)),
        }
    }
    pub fn new_witnessing(pids: Vec<dscale::Pid>) -> Self {
        Self {
            flavor: Flavor::Threshold(thres::QuorumSystem::new(pids, 5, 4)),
        }
    }
    pub fn new_witnessing_grid(pids: Vec<dscale::Pid>) -> Self {
        Self {
            flavor: Flavor::WitnessGrid(grid::QuorumSystem::new(pids)),
        }
    }
    pub fn new_witnessing_grid_with_faults(pids: Vec<dscale::Pid>, faults: usize) -> Self {
        Self {
            flavor: Flavor::WitnessGrid(grid::QuorumSystem::with_faults(pids, faults)),
        }
    }
}

impl QuorumSystem {
    pub fn choose_random_quorum(&mut self, rng: &mut impl Rng) -> &[dscale::Pid] {
        match &mut self.flavor {
            Flavor::Threshold(qs) => qs.choose_random_quorum(rng),
            Flavor::WitnessGrid(qs) => qs.choose_random_quorum(rng),
        }
    }
    pub fn size(&self) -> usize {
        match &self.flavor {
            Flavor::Threshold(qs) => qs.size(),
            Flavor::WitnessGrid(qs) => qs.size(),
        }
    }
    pub fn consensus_committee_size(&self) -> usize {
        match &self.flavor {
            Flavor::Threshold(qs) => qs.size(),
            Flavor::WitnessGrid(qs) => qs.consensus_committee_size(),
        }
    }
    pub fn is_quorum(&self, pids: impl Iterator<Item = dscale::Pid> + Clone) -> bool {
        match &self.flavor {
            Flavor::Threshold(qs) => qs.is_quorum(pids),
            Flavor::WitnessGrid(qs) => qs.is_quorum(pids),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    #[test]
    fn dissemination_size() {
        let qs = QuorumSystem::new_dissemination((0..9).collect());
        assert_eq!(qs.size(), 5); // 2 * (9 / 3) - 1
    }

    #[test]
    fn dissemination_size_for_small_clusters() {
        assert_eq!(QuorumSystem::new_dissemination((0..2).collect()).size(), 1);
        assert_eq!(QuorumSystem::new_dissemination((0..4).collect()).size(), 3);
    }

    #[test]
    fn witnessing_size() {
        let qs = QuorumSystem::new_witnessing((0..10).collect());
        assert_eq!(qs.size(), 7); // 4 * (10 / 5) - 1
    }

    #[test]
    fn witnessing_grid_size() {
        let qs = QuorumSystem::new_witnessing_grid((0..25).collect());
        assert_eq!(qs.size(), 10); // x=2, n=5 -> 2 * 5
    }

    #[test]
    fn dissemination_quorum_is_subset_of_pids() {
        let pids: Vec<dscale::Pid> = (0..9).collect();
        let mut qs = QuorumSystem::new_dissemination(pids.clone());
        let mut rng = StdRng::seed_from_u64(1);

        let expected_size = qs.size();
        let quorum = qs.choose_random_quorum(&mut rng).to_vec();

        assert_eq!(quorum.len(), expected_size);
        assert!(quorum.iter().all(|pid| pids.contains(pid)));
        let unique: HashSet<_> = quorum.iter().collect();
        assert_eq!(unique.len(), quorum.len(), "quorum must not repeat pids");
    }

    #[test]
    fn witnessing_grid_quorum_is_subset_of_pids() {
        let pids: Vec<dscale::Pid> = (0..25).collect();
        let mut qs = QuorumSystem::new_witnessing_grid(pids.clone());
        let mut rng = StdRng::seed_from_u64(7);

        let quorum = qs.choose_random_quorum(&mut rng).to_vec();

        assert!(!quorum.is_empty());
        assert!(quorum.iter().all(|pid| pids.contains(pid)));
        let unique: HashSet<_> = quorum.iter().collect();
        assert_eq!(unique.len(), quorum.len(), "quorum must not repeat pids");
    }

    #[test]
    fn is_quorum_dispatches_to_threshold_flavor() {
        let mut qs = QuorumSystem::new_dissemination((0..9).collect());
        let mut rng = StdRng::seed_from_u64(1);
        let quorum = qs.choose_random_quorum(&mut rng).to_vec();

        assert!(qs.is_quorum(quorum.iter().copied()));
        assert!(!qs.is_quorum([0, 1].into_iter()));
    }

    #[test]
    fn is_quorum_dispatches_to_grid_flavor() {
        let mut qs = QuorumSystem::new_witnessing_grid((0..25).collect());
        let mut rng = StdRng::seed_from_u64(2);
        let quorum = qs.choose_random_quorum(&mut rng).to_vec();

        assert!(qs.is_quorum(quorum.iter().copied()));
        assert!(!qs.is_quorum([0, 1].into_iter()));
    }
}
