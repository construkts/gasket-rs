use std::time::Duration;

use crossbeam::channel::{Receiver, RecvTimeoutError, Sender};

use crate::error::Error;

pub struct Message<T> {
    pub payload: T,
}

impl<T> From<T> for Message<T> {
    fn from(payload: T) -> Self {
        Message { payload }
    }
}

impl<T> Clone for Message<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            payload: self.payload.clone(),
        }
    }
}

pub trait SendPort<T> {
    fn connect(&mut self, sender: Sender<Message<T>>);
}

pub struct OutputPort<T> {
    sender: Option<Sender<Message<T>>>,
}

impl<T> Clone for OutputPort<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
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
}

impl<T> SendPort<T> for OutputPort<T> {
    fn connect(&mut self, sender: Sender<Message<T>>) {
        self.sender = Some(sender);
    }
}

pub struct FanoutPort<T>
where
    T: Clone,
{
    senders: Vec<Sender<Message<T>>>,
}

impl<T> FanoutPort<T>
where
    T: Clone,
{
    pub fn send(&mut self, msg: Message<T>) -> Result<(), Error> {
        if self.senders.is_empty() {
            return Err(Error::NotConnected);
        }

        for sender in self.senders.iter_mut() {
            sender.send(msg.clone()).map_err(|_| Error::SendError)?;
        }

        Ok(())
    }
}

impl<T> SendPort<T> for FanoutPort<T>
where
    T: Clone,
{
    fn connect(&mut self, sender: Sender<Message<T>>) {
        self.senders.push(sender);
    }
}

impl<T> Default for FanoutPort<T>
where
    T: Clone,
{
    fn default() -> Self {
        Self {
            senders: Vec::new(),
        }
    }
}

pub trait RecvPort<T> {
    fn connect(&mut self, receiver: Receiver<Message<T>>);
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

// TODO: there should a notion of what's the expected throughput for each port,
// it could be a value set at the port level. This could give us a way to
// calculate a more accurate timeout instead of relying on a magic number.
const IDLE_TIMEOUT: Duration = Duration::from_millis(2000);

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

    pub fn recv_or_idle(&mut self) -> Result<Message<T>, Error> {
        match &self.receiver {
            Some(receiver) => match receiver.recv_timeout(IDLE_TIMEOUT) {
                Ok(unit) => {
                    self.counter += 1;
                    Ok(unit)
                }
                Err(RecvTimeoutError::Timeout) => Err(Error::RecvIdle),
                Err(_) => Err(Error::RecvError),
            },
            None => Err(Error::NotConnected),
        }
    }
}

impl<T> RecvPort<T> for InputPort<T> {
    fn connect(&mut self, receiver: Receiver<Message<T>>) {
        self.receiver = Some(receiver);
    }
}

pub struct TwoPhaseInputPort<T> {
    inner: InputPort<T>,
    staging: Option<Message<T>>,
}

impl<T> TwoPhaseInputPort<T>
where
    T: Clone,
{
    pub fn recv(&mut self) -> Result<Message<T>, Error> {
        if self.staging.is_none() {
            let x = self.inner.recv()?;
            self.staging = Some(x);
        }

        let x = self.staging.as_ref().unwrap();
        Ok(x.clone())
    }

    pub fn recv_or_idle(&mut self) -> Result<Message<T>, Error> {
        if self.staging.is_none() {
            let x = self.inner.recv()?;
            self.staging = Some(x);
        }

        let x = self.staging.as_ref().unwrap();
        Ok(x.clone())
    }

    pub fn commit(&mut self) {
        self.staging = None;
    }
}

impl<T> RecvPort<T> for TwoPhaseInputPort<T> {
    fn connect(&mut self, receiver: Receiver<Message<T>>) {
        self.inner.connect(receiver);
    }
}

impl<T> Default for TwoPhaseInputPort<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            staging: Default::default(),
        }
    }
}

pub struct FunnelPort<T> {
    receivers: Vec<Receiver<Message<T>>>,
}

impl<T> FunnelPort<T> {
    pub fn recv(&mut self) -> Result<Message<T>, Error> {
        let mut select = crossbeam::channel::Select::new();

        for recv in self.receivers.iter() {
            select.recv(recv);
        }

        loop {
            // Wait until a receive operation becomes ready and try executing it.
            let index = select.ready();

            let res = self.receivers[index].try_recv();

            // If the operation turns out not to be ready, retry.
            if let Err(e) = res {
                if e.is_empty() {
                    continue;
                }
            }

            // Success!
            return res.map_err(|_| Error::RecvError);
        }
    }
}

impl<T> RecvPort<T> for FunnelPort<T> {
    fn connect(&mut self, receiver: Receiver<Message<T>>) {
        self.receivers.push(receiver);
    }
}

impl<T> Default for FunnelPort<T> {
    fn default() -> Self {
        Self {
            receivers: Vec::new(),
        }
    }
}

pub fn connect_ports<T>(output: &mut impl SendPort<T>, input: &mut impl RecvPort<T>, cap: usize) {
    let (sender, receiver) = crossbeam::channel::bounded(cap);
    output.connect(sender);
    input.connect(receiver);
}
