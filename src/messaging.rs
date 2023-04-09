use std::{collections::VecDeque, marker::PhantomData};

use crate::error::Error;

#[derive(Debug, Default)]
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

pub trait SendAdapter<P>: Send + Sync {
    async fn send(&mut self, msg: Message<P>) -> Result<(), Error>;
}

pub trait SendPort<A, P>
where
    A: SendAdapter<P>,
{
    fn connect(&mut self, adapter: A);
}

pub struct OutputPort<A, P> {
    sender: Option<A>,
    _phantom: PhantomData<P>,
}

impl<A, P> Clone for OutputPort<A, P>
where
    A: Clone,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<A, P> Default for OutputPort<A, P> {
    fn default() -> Self {
        Self {
            sender: None,
            _phantom: Default::default(),
        }
    }
}

impl<A, P> OutputPort<A, P>
where
    A: SendAdapter<P>,
{
    pub async fn send(&mut self, msg: Message<P>) -> Result<(), Error> {
        match &mut self.sender {
            Some(sender) => sender.send(msg).await,
            None => Err(Error::NotConnected),
        }
    }
}

impl<A, P> SendPort<A, P> for OutputPort<A, P>
where
    A: SendAdapter<P>,
{
    fn connect(&mut self, adapter: A) {
        self.sender = Some(adapter);
    }
}

pub struct FanoutPort<A, P>
where
    A: Clone,
{
    senders: Vec<A>,
    _phantom: PhantomData<P>,
}

impl<A, P> FanoutPort<A, P>
where
    A: SendAdapter<P> + Clone,
    P: Clone,
{
    pub async fn send(&mut self, msg: Message<P>) -> Result<(), Error> {
        if self.senders.is_empty() {
            return Err(Error::NotConnected);
        }

        for sender in self.senders.iter_mut() {
            sender.send(msg.clone()).await?;
        }

        Ok(())
    }
}

impl<A, P> SendPort<A, P> for FanoutPort<A, P>
where
    A: SendAdapter<P> + Clone,
{
    fn connect(&mut self, adapter: A) {
        self.senders.push(adapter);
    }
}

impl<A, P> Default for FanoutPort<A, P>
where
    A: SendAdapter<P> + Clone,
{
    fn default() -> Self {
        Self {
            senders: Vec::new(),
            _phantom: Default::default(),
        }
    }
}

pub trait RecvAdapter<P>: Send + Sync {
    async fn recv(&mut self) -> Result<Message<P>, Error>;
}

pub trait RecvPort<A, P>
where
    A: RecvAdapter<P>,
{
    fn connect(&mut self, adapter: A);
}

pub struct InputPort<A, P>
where
    A: RecvAdapter<P>,
{
    counter: u64,
    receiver: Option<A>,
    _phantom: PhantomData<P>,
}

impl<A, P> Default for InputPort<A, P>
where
    A: RecvAdapter<P>,
{
    fn default() -> Self {
        Self {
            counter: 0,
            receiver: Default::default(),
            _phantom: Default::default(),
        }
    }
}

impl<A, P> Clone for InputPort<A, P>
where
    A: RecvAdapter<P> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            counter: self.counter.clone(),
            receiver: self.receiver.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<A, P> InputPort<A, P>
where
    A: RecvAdapter<P>,
{
    pub async fn recv(&mut self) -> Result<Message<P>, Error> {
        let receiver = self.receiver.as_mut().ok_or(Error::NotConnected)?;
        let msg = receiver.recv().await?;
        self.counter += 1;

        Ok(msg)
    }
}

impl<A, P> RecvPort<A, P> for InputPort<A, P>
where
    A: RecvAdapter<P>,
{
    fn connect(&mut self, adapter: A) {
        self.receiver = Some(adapter);
    }
}

pub struct SinkAdapter<P> {
    cap: Option<usize>,
    buffer: VecDeque<Message<P>>,
}

impl<P> SendAdapter<P> for SinkAdapter<P>
where
    P: Send + Sync,
{
    async fn send(&mut self, msg: Message<P>) -> Result<(), Error> {
        self.buffer.push_back(msg);

        if let Some(cap) = self.cap {
            while self.buffer.len() > cap {
                self.buffer.pop_back();
            }
        }

        Ok(())
    }
}

impl<P> SinkAdapter<P> {
    pub fn new(cap: Option<usize>) -> Self {
        Self {
            cap,
            buffer: Default::default(),
        }
    }

    pub fn drain(&mut self) -> Vec<Message<P>> {
        self.buffer.drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

pub struct MapSendAdapter<I, F, T>
where
    I: SendAdapter<T>,
{
    inner: I,
    mapper: fn(F) -> Option<T>,
}

impl<I, F, T> MapSendAdapter<I, F, T>
where
    I: SendAdapter<T>,
{
    pub fn new(inner: I, mapper: fn(F) -> Option<T>) -> Self {
        Self { inner, mapper }
    }
}

impl<I, F, T> SendAdapter<F> for MapSendAdapter<I, F, T>
where
    I: SendAdapter<T>,
{
    async fn send(&mut self, msg: Message<F>) -> Result<(), Error> {
        let out = (self.mapper)(msg.payload);

        if let Some(payload) = out {
            self.inner.send(Message::from(payload)).await?;
        }

        Ok(())
    }
}

pub mod tokio {
    use super::*;

    use ::tokio::sync::mpsc::{Receiver, Sender};

    pub struct ChannelSendAdapter<P>(Sender<Message<P>>);

    impl<P> Clone for ChannelSendAdapter<P> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<P> SendAdapter<P> for ChannelSendAdapter<P>
    where
        P: Send + Sync,
    {
        async fn send(&mut self, msg: Message<P>) -> Result<(), Error> {
            self.0.send(msg).await.map_err(|_| Error::SendError)
        }
    }

    pub struct ChannelRecvAdapter<P>(Receiver<Message<P>>);

    impl<P> RecvAdapter<P> for ChannelRecvAdapter<P>
    where
        P: Send + Sync,
    {
        async fn recv(&mut self) -> Result<Message<P>, Error> {
            match self.0.recv().await {
                Some(x) => Ok(x),
                None => Err(Error::RecvError),
            }
        }
    }

    pub type OutputPort<P> = super::OutputPort<ChannelSendAdapter<P>, P>;
    pub type InputPort<P> = super::InputPort<ChannelRecvAdapter<P>, P>;
    pub type MapSendAdapter<F, T> = super::MapSendAdapter<ChannelSendAdapter<T>, F, T>;

    pub fn channel<P>(cap: usize) -> (ChannelSendAdapter<P>, ChannelRecvAdapter<P>) {
        let (sender, receiver) = ::tokio::sync::mpsc::channel(cap);
        (ChannelSendAdapter(sender), ChannelRecvAdapter(receiver))
    }

    pub fn connect_ports<O, I, P>(output: &mut O, input: &mut I, cap: usize)
    where
        O: SendPort<ChannelSendAdapter<P>, P>,
        I: RecvPort<ChannelRecvAdapter<P>, P>,
        P: 'static + Send + Sync,
    {
        let (sender, receiver) = channel::<P>(cap);
        output.connect(sender);
        input.connect(receiver);
    }

    pub fn funnel_ports<O, I, P>(outputs: Vec<&mut O>, input: &mut I, cap: usize)
    where
        O: SendPort<ChannelSendAdapter<P>, P>,
        I: RecvPort<ChannelRecvAdapter<P>, P>,
        P: 'static + Send + Sync,
    {
        let (sender, receiver) = channel::<P>(cap);
        input.connect(receiver);

        for output in outputs {
            output.connect(sender.clone());
        }
    }
}
