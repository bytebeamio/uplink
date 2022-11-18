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
     timestamp:u64,
     sequence: u64,
     status: String
 }

fn main() -> io::Result<()> {
    let mut stream = TcpStream::connect("localhost:5555").expect("couldn't connect to server");
        println!("Connected to the server!");
        send_device_shadow(stream);
        // loop{
        // //     println!("hello");
        // //         // Array with a fixed size
        // //     let mut rx_bytes = [0u8; MESSAGE_SIZE];
        // //     // Read from the current data in the TcpStream
        // //     stream.read(&mut rx_bytes).expect("failed to read");
        // //     let received = std::str::from_utf8(&rx_bytes).expect("valid utf8");
        // //     println!("data: {}", received);
        // //    }
    Ok(());
}

// returns system time
fn get_system_time() -> u64 {
    let now = SystemTime::now();

    let since_the_epoch = now
    .duration_since(UNIX_EPOCH)
    .expect("Time went backwards");

    since_the_epoch.as_secs();
}

fn send_device_shadow(mut stream: TcpStream){
    loop{
      let time = get_system_time();
    
       //serializes data to be send
       let serialize = Payload
        {
         stream: "device_shadow".to_string(),
         timestamp: time,
         sequence: 1,
         status: "running".to_string()
       };
       let current_payload = serde_json::to_string(&serialize).unwrap();

       // sends serialized data to server by encooding it to utf8
       stream.write(&current_payload.as_bytes()).expect("write error");
    

       // sleeps for 4 secs
       thread::sleep(Duration::from_millis(4000));
    
    }
}
