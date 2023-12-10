use std::time::Duration;

use tokio::sync::mpsc::channel;

#[tokio::main]
async fn main() {
    let (t1, mut r1) = channel(1);
    let (t2, _) = channel(10);
    {
        let t2 = t2.clone();
        tokio::spawn(async move {
            loop {
                let value = match r1.recv().await {
                    Some(value) => value,
                    _ => break,
                };
                first_response(value);
                match t2.send(value).await {
                    Ok(value) => value,
                    Err(_) => break,
                }
            }
        });
    }
    // tokio::spawn(async move {
    //     loop {
    //         let value = match r2.recv_async().await {
    //             Ok(value) => value,
    //             Err(_) => break,
    //         };
    //         second_response(value);
    //     }
    // });

    tokio::spawn(async move {
        let mut idx = 1;
        loop {
            match t1.send(idx).await {
                Ok(value) => value,
                Err(_) => break,
            };
            idx += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .unwrap();
}

fn first_response(value: u32) {
    println!("first_response: {}", value);
}

// fn second_response(value: u32) {
//     println!("second_response: {}", value);
// }
