use dscale::rand::Rng;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct CmdId {
    pub pid: dscale::Pid,
    pub id: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct Command {
    pub id: CmdId,  // unique id
    pub key: usize, // conflicts
}

pub fn create_cmd(rng: &mut impl Rng, key_count: usize) -> Command {
    use dscale::rand::prelude::IteratorRandom;
    Command {
        id: CmdId {
            pid: dscale::pid(),
            id: dscale::unique_id(),
        },
        key: (0..key_count).choose(rng).expect("choose failed"),
    }
}

impl Command {
    pub fn conflicts_with(&self, other_cmd: &Command) -> bool {
        self.key == other_cmd.key
    }
}
