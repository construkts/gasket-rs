use std::collections::VecDeque;

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

pub struct OutputPort<P> {
    sender: Option<Box<dyn SendAdapter<P>>>,
}

impl<P> OutputPort<P> {
    fn connect(&mut self, adapter: impl SendAdapter<P> + 'static) {
        self.sender = Some(Box::new(adapter));
    }

    pub async fn send(&mut self, msg: Message<P>) -> Result<(), Error> {
        match &mut self.sender {
            Some(sender) => sender.send(msg).await,
            None => Err(Error::NotConnected),
        }
    }
}

impl<P> Default for OutputPort<P> {
    fn default() -> Self {
        Self { sender: None }
    }
}

#[derive(Default)]
pub struct Fanout<P> {
    senders: Vec<OutputPort<P>>,
}

impl<P> Fanout<P>
where
    P: Clone,
{
    pub fn new(&mut self, mut ports: Vec<OutputPort<P>>) {
        self.senders.append(&mut ports);
    }

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

#[async_trait::async_trait]
pub trait RecvAdapter<P>: Send + Sync
where
    P: Send + Sync + Clone,
{
    async fn recv(&mut self) -> Result<Message<P>, Error>;
}

pub struct InputPort<P> {
    receiver: Option<Box<dyn RecvAdapter<P>>>,
}

impl<P> Default for InputPort<P> {
    fn default() -> Self {
        Self {
            receiver: Default::default(),
        }
    }
}

impl<P> InputPort<P>
where
    P: Send + Sync + Clone,
{
    fn connect(&mut self, adapter: impl Sized + RecvAdapter<P> + 'static) {
        self.receiver = Some(Box::new(adapter));
    }

    pub async fn recv(&mut self) -> Result<Message<P>, Error> {
        let receiver = self.receiver.as_mut().ok_or(Error::NotConnected)?;
        let msg = receiver.recv().await?;

        Ok(msg)
    }
}

struct RunningTimer {
    thread: ::tokio::task::JoinHandle<()>,
    recv: ::tokio::sync::watch::Receiver<std::time::Instant>,
}

impl RunningTimer {
    pub fn start(interval: std::time::Duration) -> Self {
        let (send, recv) = ::tokio::sync::watch::channel(std::time::Instant::now());

        let thread = ::tokio::spawn(async move {
            loop {
                ::tokio::time::sleep(interval).await;
                let res = send.send(std::time::Instant::now());
                if let Err(err) = res {
                    tracing::warn!(?err, "timer send error");
                    break;
                }
            }
        });

        RunningTimer { recv, thread }
    }
}

pub struct TimerPort {
    interval: std::time::Duration,
    running: Option<RunningTimer>,
}

impl TimerPort {
    pub fn new(interval: std::time::Duration) -> Self {
        Self {
            interval,
            running: None,
        }
    }

    pub fn stop(&mut self) {
        if let Some(running) = self.running.take() {
            running.thread.abort();
        }
    }

    pub fn from_secs(secs: u64) -> Self {
        Self::new(std::time::Duration::from_secs(secs))
    }

    pub async fn recv(&mut self) -> Result<std::time::Instant, Error> {
        if self.running.is_none() {
            let running = RunningTimer::start(self.interval.clone());
            self.running = Some(running);
        }

        let running = self.running.as_mut().ok_or(Error::NotConnected)?;

        running.recv.changed().await.map_err(|_| Error::RecvError)?;
        Ok(running.recv.borrow().clone())
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

pub struct OutputMap<In, Out> {
    mapper: fn(In) -> Option<Out>,
    inner: OutputPort<Out>,
}

impl<In, Out> OutputMap<In, Out> {
    pub fn new(inner: OutputPort<Out>, mapper: fn(In) -> Option<Out>) -> Self {
        Self { inner, mapper }
    }

    pub async fn send(&mut self, msg: Message<In>) -> Result<(), Error> {
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

    pub fn connect_ports<P>(output: &mut OutputPort<P>, input: &mut InputPort<P>, cap: usize)
    where
        P: Send + Sync + Clone + 'static,
    {
        let (sender, receiver) = mpsc_channel::<P>(cap);
        output.connect(sender);
        input.connect(receiver);
    }

    pub fn funnel_ports<P>(outputs: Vec<&mut OutputPort<P>>, input: &mut InputPort<P>, cap: usize)
    where
        P: 'static + Send + Sync + Clone,
    {
        let (sender, receiver) = mpsc_channel::<P>(cap);
        input.connect(receiver);

        for output in outputs {
            output.connect(sender.clone());
        }
    }

    pub fn broadcast_port<P>(output: &mut OutputPort<P>, inputs: Vec<&mut InputPort<P>>, cap: usize)
    where
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::messaging::TimerPort;

    #[::tokio::test]
    #[ignore = "not ready"]
    async fn test_timer_port() {
        let mut input = TimerPort::new(Duration::from_secs(2));

        let start = Instant::now();

        loop {
            let timer = input.recv().await.unwrap();
            tokio::time::sleep(Duration::from_millis(2300)).await;
            println!(
                "timer: {}, elapsed: {}",
                timer.elapsed().as_secs(),
                start.elapsed().as_secs()
            );
        }
    }
}
