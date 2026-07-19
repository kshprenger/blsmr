use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dscale::{Message, MessagePtr, Pid, broadcast, pid, send, unique_id};

#[derive(Clone, PartialEq, Eq, Hash, Copy, Debug)]
pub(crate) struct MessageId {
    process_id: Pid,
    message_id: usize,
}

#[derive(Debug)]
pub enum BCBMessage {
    Initiate((MessageId, Arc<dyn Message>)),
    Signature(MessageId),
    Certificate(usize, MessageId),
}

impl Message for BCBMessage {
    fn virtual_size(&self) -> usize {
        match self {
            Self::Initiate((_, message)) => 128 + message.virtual_size(),
            Self::Signature(_) => 64,
            Self::Certificate(validators, _) => 128 + validators / 8,
        }
    }
}

#[derive(Default)]
pub struct ByzantineConsistentBroadcast {
    messages: HashMap<MessageId, (Arc<dyn Message>, usize)>,
    waiting_certificates: HashSet<MessageId>,
    proc_num: usize,
}

impl ByzantineConsistentBroadcast {
    pub fn on_start(&mut self, proc_num: usize) {
        self.proc_num = proc_num;
    }

    pub fn reliably_broadcast(&mut self, message: impl Message + 'static) {
        let id = MessageId {
            process_id: pid(),
            message_id: unique_id(),
        };
        let message = Arc::new(message);
        self.messages.insert(id, (message.clone(), 0));
        broadcast(BCBMessage::Initiate((id, message)));
    }

    pub fn on_message(&mut self, from: Pid, message: &BCBMessage) -> Option<MessagePtr> {
        match message {
            BCBMessage::Certificate(_, id) => match self.messages.remove(id) {
                None => {
                    self.waiting_certificates.insert(*id);
                    None
                }
                Some((message, _)) => Some(MessagePtr::Shared(message)),
            },
            BCBMessage::Initiate((id, message)) => {
                if id.process_id != pid() {
                    if self.waiting_certificates.remove(id) {
                        return Some(MessagePtr::Shared(message.clone()));
                    }
                    self.messages.insert(*id, (message.clone(), 0));
                }
                send(from, BCBMessage::Signature(*id));
                None
            }
            BCBMessage::Signature(id) => {
                let message = self.messages.get_mut(id)?;
                message.1 += 1;
                if message.1 == self.quorum_size() {
                    broadcast(BCBMessage::Certificate(self.proc_num, *id));
                }
                None
            }
        }
    }

    fn quorum_size(&self) -> usize {
        2 * ((self.proc_num - 1) / 3) + 1
    }
}
