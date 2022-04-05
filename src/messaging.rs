use crossbeam::channel::{Receiver, Sender};

use crate::error::Error;

pub struct Message<T> {
    pub payload: T,
}

impl<T> From<T> for Message<T> {
    fn from(payload: T) -> Self {
        Message { payload }
    }
}

pub struct OutputPort<T> {
    sender: Option<Sender<Message<T>>>,
}

impl<T> Default for OutputPort<T> {
    fn default() -> Self {
        Self {
            sender: Default::default(),
        }
    }
}

impl<T> OutputPort<T> {
    pub fn send(&mut self, msg: Message<T>) -> Result<(), Error> {
        match &self.sender {
            Some(sender) => sender.send(msg).map_err(|_| Error::SendError),
            None => Err(Error::NotConnected),
        }
    }

    fn connect(&mut self, sender: Sender<Message<T>>) {
        self.sender = Some(sender);
    }
}

pub struct InputPort<T> {
    counter: u64,
    receiver: Option<Receiver<Message<T>>>,
}

impl<T> Default for InputPort<T> {
    fn default() -> Self {
        Self {
            counter: 0,
            receiver: Default::default(),
        }
    }
}

impl<T> InputPort<T> {
    pub fn recv(&mut self) -> Result<Message<T>, Error> {
        match &self.receiver {
            Some(receiver) => match receiver.recv() {
                Ok(unit) => {
                    self.counter += 1;
                    Ok(unit)
                }
                Err(_) => Err(Error::RecvError),
            },
            None => Err(Error::NotConnected),
        }
    }

    fn connect(&mut self, receiver: Receiver<Message<T>>) {
        self.receiver = Some(receiver);
    }
}

pub fn connect_ports<T>(output: &mut OutputPort<T>, input: &mut InputPort<T>, cap: usize) {
    let (sender, receiver) = crossbeam::channel::bounded(cap);
    output.connect(sender);
    input.connect(receiver);
}
