use client::Command;

#[derive(Default)]
pub struct CmdLog {
    append_only_log: Vec<Command>,
}

impl CmdLog {
    pub fn new() -> Self {
        Self {
            append_only_log: Vec::new(),
        }
    }
    pub fn conflicts(&mut self, c: Command) -> Vec<Command> {
        let conflicts = self
            .append_only_log
            .iter()
            .cloned()
            .filter(|cmd| cmd.conflicts_with(&c))
            .collect();
        self.append_only_log.push(c);
        conflicts
    }
}
