#![allow(unused)]
use std::io::{self, Read};
use std::io::prelude::*;
use std::time::{SystemTime,UNIX_EPOCH};
use std::net::TcpStream;
use serde::{Deserialize, Serialize};
use std::{thread, time::Duration};
const MESSAGE_SIZE: usize = 5;

#[derive(Serialize, Deserialize, Debug)]
 struct Payload{
     stream:String,
     timestamp:String,
     sequence: String,
     status: String
 }

fn main() -> io::Result<()> {
    let mut stream = TcpStream::connect("localhost:5555").expect("couldn't connect to server");
        println!("Connected to the server!");
        send_device_shadow();
        loop{
            println!("hello");
                // Array with a fixed size
            let mut rx_bytes = [0u8; MESSAGE_SIZE];
            // Read from the current data in the TcpStream
            stream.read(&mut rx_bytes).expect("failed to read");
            let received = std::str::from_utf8(&rx_bytes).expect("valid utf8");
            println!("data: {}", received);
           }
    Ok(())
}

fn send_device_shadow(){
    loop{
    let now = SystemTime::now();
    let since_the_epoch = now
    .duration_since(UNIX_EPOCH)
    .expect("Time went backwards");
    let serialize = Payload
    {
    stream: "device_shadow".to_string(),
    timestamp: since_the_epoch.as_secs().to_string(),
    sequence: "test".to_string(),
    status: "running".to_string()
    };
    let current_payload = serde_json::to_string(&serialize).unwrap();
    println!("deserialised: {}", current_payload);
    thread::sleep(Duration::from_millis(4000));
    }
}
