#![allow(unused)]
use std::io::{self, Read};
use std::io::prelude::*;
use std::time::{SystemTime,UNIX_EPOCH};
use std::net::TcpStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{thread, time::Duration};
use std::io::BufReader;


#[derive(Serialize, Deserialize, Debug)]
 struct Payload{
     stream:String,
     sequence: u32,
     timestamp:u64,
     status: String
 }

fn main() -> io::Result<()> {
    let mut stream = TcpStream::connect("localhost:5555").expect("couldn't connect to server");
    let mut stream_clone = stream.try_clone().expect("clone failed...");
    println!("Connected to the server!");
 
    // Thread for sending data
    thread::spawn(move || {
        send_device_shadow(stream);
    });
    
    // receives and prints json data
    let mut line = [0;2048];
    loop{
       let result = stream_clone.read(&mut line)?;
       let deserialized:Value = serde_json::from_slice(&line[0..result]).unwrap();
       println!("Received data: {:?}", &deserialized);
       thread::sleep(Duration::from_millis(1)); 
    }
    Ok(())
}

// returns system time
fn get_system_time() -> u64 {
    let now = SystemTime::now();

    let since_the_epoch = now
    .duration_since(UNIX_EPOCH)
    .expect("Time went backwards");

    since_the_epoch.as_secs()
}

// sends data to server
fn send_device_shadow(mut stream: TcpStream){
    let mut seq:u32 = 1;
    loop{
      let time = get_system_time();
    
       //serializes data to be send
       let serialize = Payload
        {
         stream: "device_shadow".to_string(),
         sequence: seq,
         timestamp: time,
         status: "running".to_string()
       };
       let current_payload = serde_json::to_string(&serialize).unwrap();

       // sends serialized data to server
       stream.write(&current_payload.as_bytes()).expect("write error");
    
       seq+=1; 
       println!("wrote data");
       // sleeps for 4 secs
       thread::sleep(Duration::from_millis(4000));
    
    }
}
