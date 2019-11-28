use derive_more::From;

pub struct Can {
    bus: socketcan::CANSocket
}

#[derive(From, Debug)]
pub enum Error {
    Can(socketcan::CANSocketOpenError)
}

impl Can {
    pub fn new(ifname: &str) -> Result<Can, Error> {
        let bus = socketcan::CANSocket::open(ifname)?;

        let can = Can {
            bus
        };

        Ok(can)
    }

    pub fn start(&mut self) {
        loop {
            let frame = self.bus.read_frame().unwrap();
            println!("Frame = {:?}", frame);
        }
    }
}