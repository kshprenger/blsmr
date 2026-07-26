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
