use std::io::{self, Read, BufReader, prelude::*};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::net::TcpStream;
use std::thread;



fn main() -> io::Result<()> {
    let mut stream = TcpStream::connect("localhost:5555").expect("couldn't connect to server");
    let mut stream_clone = stream.try_clone().expect("clone failed...");
    println!("Connected to the server!");
 
    // Thread for sending data
    thread::spawn(move || {
        send_device_shadow(stream_clone);
    });
    
    // receives and prints json data
    let mut line = [0;2048];
    loop{
       let result = stream.read(&mut line)?;
       let data = str::from_utf8(&line[0..result]).unwrap();
       println!("Received data: {}", data);
       thread::sleep(Duration::from_millis(1)); 
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
       let serialize = format!(
              r#"{{"stream": "device_shadow", "sequence": {seq},"timestamp": {time},"status": "running"}}"#
          ) + "\n";


       // sends data to server
       stream.write(serialize.as_bytes()).expect("write error");
       stream.flush().unwrap();

       seq+=1; 
       println!("wrote data");
       // sleeps for 4 secs
       thread::sleep(Duration::from_millis(4000));
    
    }
}
