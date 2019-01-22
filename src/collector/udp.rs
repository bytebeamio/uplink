use super::error::CollectorError;
use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use tokio_current_thread;
use tokio_udp::UdpSocket;
use tokio_codec::LinesCodec;
use tokio_udp::UdpFramed;
use futures::stream::Stream;
use futures::{future, Future};

type Data = String;

pub struct Udp<'a> {
    /// channel, channel data, count
    event_data: HashMap<&'a str, (Vec<Data>, usize)>,
    /// channel, channel limit
    event_limit: HashMap<&'a str, usize>,
    /// duration to check for channel progress or flush
    timeout: Duration,
    /// channel to flush the data to
    tx: Sender<Vec<Data>>,
}

impl<'a> Udp<'a> {
    pub fn new(tx: Sender<Vec<Data>>) -> Udp<'a> {
        Udp {
            event_data: HashMap::new(),
            event_limit: HashMap::new(),
            timeout: Duration::from_secs(10),
            tx,
        }
    }

    pub fn set_channel_limits(mut self, limits: HashMap<&'a str, usize>) -> Udp<'a> {
        for (channel, limit) in limits.into_iter() {
            self.event_data.insert(channel, (Vec::new(), 0));
            self.event_limit.insert(channel, limit);
        }

        self
    }

    pub fn set_timeout(mut self, duration: Duration) -> Udp<'a> {
        self.timeout = duration;
        self
    }

    fn fill_buffer(&mut self, channel: &'a str, data: Data) -> Option<Vec<Data>> {
        let event_data = &mut self.event_data;

        let count = match event_data.get_mut(channel) {
            Some((buffer, count)) => {
                buffer.push(data);
                *count += 1;
                *count
            }
            None => {
                let data = (vec![data], 1);
                event_data.insert(channel, data);
                1
            }
        };

        if count >= 10 {
            Some(event_data.remove(channel).unwrap().0)
        } else {
            None
        }
    }

    fn mock_start(mut self) -> Result<(), CollectorError> {
        for i in 0..100 {
            match self.fill_buffer("can_raw", "can data".to_owned()) {
                Some(data) => self.tx.send(data).unwrap(),
                None => {
                    thread::sleep_ms(300);
                    continue;
                }
            }
        }

        Ok(())
    }

    pub fn start(mut self) -> Result<(), CollectorError> {
        let addr: SocketAddr = "127.0.0.1:0".parse()?;
        let socket = UdpSocket::bind(&addr)?;
        let (sink, stream) = UdpFramed::new(socket, LinesCodec::new()).split();
        let (tx, rx) = futures::sync::mpsc::channel::<i32>(10);
        
        let udp = stream
            .map_err(|e| CollectorError::Io(e))
            .for_each(|(data, _s)| {
            match self.fill_buffer("can_raw", data) {
                Some(data) => {
                    self.tx.try_send(data).unwrap();
                    future::ok(())
                },
                None => future::ok(())
            }
        });

        let requests = rx
        .map_err(|e| CollectorError::Channel)
        .for_each(|v| {
            self.event_limit.insert("can_raw", 32);
            future::ok(())
        });

        let f = udp.select(requests)
                .map(|(v, _select)| v)
                .map_err(|(err, _select)| err);

        tokio_current_thread::block_on_all(f)
    }
}
