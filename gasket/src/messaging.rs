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

#[async_trait::async_trait]
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
            _phantom: self._phantom,
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

#[async_trait::async_trait]
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
    receiver: Option<A>,
    _phantom: PhantomData<P>,
}

impl<A, P> Default for InputPort<A, P>
where
    A: RecvAdapter<P>,
{
    fn default() -> Self {
        Self {
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
            receiver: self.receiver.clone(),
            _phantom: self._phantom,
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

#[async_trait::async_trait]
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

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
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

#[async_trait::async_trait]
impl<I, F, T> SendAdapter<F> for MapSendAdapter<I, F, T>
where
    I: SendAdapter<T> + Send,
    F: Send,
    T: Send,
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

    use ::tokio::sync;

    #[derive(Clone)]
    pub enum ChannelSendAdapter<P> {
        Mpsc(sync::mpsc::Sender<Message<P>>),
        Broadcast(sync::broadcast::Sender<Message<P>>),
    }

    #[async_trait::async_trait]
    impl<P> SendAdapter<P> for ChannelSendAdapter<P>
    where
        P: Send + Sync,
    {
        async fn send(&mut self, msg: Message<P>) -> Result<(), Error> {
            match self {
                ChannelSendAdapter::Mpsc(x) => {
                    x.send(msg).await.map_err(|_| Error::SendError)?;
                }
                ChannelSendAdapter::Broadcast(x) => {
                    x.send(msg).map_err(|_| Error::SendError)?;
                }
            }

            Ok(())
        }
    }

    pub enum ChannelRecvAdapter<P> {
        Mpsc(sync::mpsc::Receiver<Message<P>>),
        Broadcast(sync::broadcast::Receiver<Message<P>>),
    }

    #[async_trait::async_trait]
    impl<P> RecvAdapter<P> for ChannelRecvAdapter<P>
    where
        P: Send + Sync + Clone,
    {
        async fn recv(&mut self) -> Result<Message<P>, Error> {
            match self {
                ChannelRecvAdapter::Mpsc(x) => match x.recv().await {
                    Some(x) => Ok(x),
                    None => Err(Error::RecvError),
                },
                ChannelRecvAdapter::Broadcast(x) => match x.recv().await {
                    Ok(x) => Ok(x),
                    Err(_) => Err(Error::RecvError),
                },
            }
        }
    }

    impl<P> From<ChannelRecvAdapter<P>> for sync::broadcast::Receiver<Message<P>> {
        fn from(value: ChannelRecvAdapter<P>) -> Self {
            match value {
                ChannelRecvAdapter::Broadcast(x) => x,
                _ => panic!("only broadcast variant receivers can used"),
            }
        }
    }

    impl<P> Clone for ChannelRecvAdapter<P>
    where
        P: Clone,
    {
        fn clone(&self) -> Self {
            match self {
                Self::Broadcast(x) => Self::Broadcast(x.resubscribe()),
                _ => panic!("only broadcast variant receivers can be cloned"),
            }
        }
    }

    pub type OutputPort<P> = super::OutputPort<ChannelSendAdapter<P>, P>;
    pub type InputPort<P> = super::InputPort<ChannelRecvAdapter<P>, P>;
    pub type MapSendAdapter<F, T> = super::MapSendAdapter<ChannelSendAdapter<T>, F, T>;

    #[deprecated(note = "use mpsc_channel instead")]
    pub fn channel<P>(cap: usize) -> (ChannelSendAdapter<P>, ChannelRecvAdapter<P>) {
        mpsc_channel(cap)
    }

    pub fn mpsc_channel<P>(cap: usize) -> (ChannelSendAdapter<P>, ChannelRecvAdapter<P>) {
        let (sender, receiver) = ::tokio::sync::mpsc::channel(cap);

        (
            ChannelSendAdapter::Mpsc(sender),
            ChannelRecvAdapter::Mpsc(receiver),
        )
    }

    pub fn broadcast_channel<P: Clone>(
        cap: usize,
    ) -> (ChannelSendAdapter<P>, ChannelRecvAdapter<P>) {
        let (sender, receiver) = ::tokio::sync::broadcast::channel(cap);

        (
            ChannelSendAdapter::Broadcast(sender),
            ChannelRecvAdapter::Broadcast(receiver),
        )
    }

    pub fn connect_ports<O, I, P>(output: &mut O, input: &mut I, cap: usize)
    where
        O: SendPort<ChannelSendAdapter<P>, P>,
        I: RecvPort<ChannelRecvAdapter<P>, P>,
        P: 'static + Send + Sync + Clone,
    {
        let (sender, receiver) = mpsc_channel::<P>(cap);
        output.connect(sender);
        input.connect(receiver);
    }

    pub fn funnel_ports<O, I, P>(outputs: Vec<&mut O>, input: &mut I, cap: usize)
    where
        O: SendPort<ChannelSendAdapter<P>, P>,
        I: RecvPort<ChannelRecvAdapter<P>, P>,
        P: 'static + Send + Sync + Clone,
    {
        let (sender, receiver) = mpsc_channel::<P>(cap);
        input.connect(receiver);

        for output in outputs {
            output.connect(sender.clone());
        }
    }

    pub fn broadcast_port<O, I, P>(output: &mut O, inputs: Vec<&mut I>, cap: usize)
    where
        O: SendPort<ChannelSendAdapter<P>, P>,
        I: RecvPort<ChannelRecvAdapter<P>, P>,
        P: 'static + Send + Sync + Clone,
    {
        let (sender, receiver) = broadcast_channel::<P>(cap);
        output.connect(sender);

        for input in inputs {
            let rx2 = receiver.clone();
            input.connect(rx2);
        }
    }
}
