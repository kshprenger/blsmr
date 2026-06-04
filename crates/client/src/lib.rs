use dscale::rand::{Rng, seq::IndexedRandom};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct CmdId {
    pub pid: dscale::Pid,
    pub id: usize,
}

#[derive(Debug, Clone)]
pub struct Command {
    pub id: CmdId,  // unique id
    pub key: usize, // conflicts
}

pub const KEY_SET: [usize; 5] = [1, 2, 3, 4, 5];

pub fn create_cmd(rng: &mut impl Rng) -> Command {
    Command {
        id: CmdId {
            pid: dscale::pid(),
            id: dscale::unique_id(),
        },
        key: *KEY_SET.choose(rng).expect("choose failed"),
    }
}

impl Command {
    pub fn conflicts_with(&self, other_cmd: &Command) -> bool {
        self.key == other_cmd.key
    }
}
