use std::io::{prelude::*, BufReader, Result};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() -> Result<()> {
    let mut stream = TcpStream::connect("localhost:5050").expect("couldn't connect to server");
    let stream_clone = stream.try_clone().expect("clone failed...");
    println!("Connected to the server!");
 
    // Thread for sending data
    thread::spawn(move || {
        send_device_shadow(stream_clone);
    });

    // receives and prints data
    let stream = BufReader::new(&mut stream);
    for line in stream.lines() {
        println!("Received data: {}", line?);
    }
    
    Ok(())
}
// sends data to server
fn send_device_shadow(mut stream: TcpStream){
    let mut seq:u32 = 1;
    loop{
      let time = 
           SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
    
       // JSON data to be sent to uplink
       let payload = format!(
              r#"{{"stream": "device_shadow", "sequence": {seq},"timestamp": {time},"status": "running"}}"#
          ) + "\n";

       // sends data to server
       stream.write(payload.as_bytes()).expect("write error");
       stream.flush().unwrap();

       seq+=1; 
       println!("wrote data");
       // sleeps for 4 secs
       thread::sleep(Duration::from_millis(4000));
    }
}
