use std::time::Duration;

#[tokio::main]
async fn main() {
    let (t1, r1) = flume::bounded(1);
    let (t2, _) = flume::bounded(10);
    {
        let r1 = r1.clone();
        let t2 = t2.clone();
        tokio::spawn(async move {
            loop {
                let Ok(value) = r1.recv_async().await else { break };
                first_response(value);
                if t2.send_async(value).await.is_err() {
                    break;
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
            if t1.send_async(idx).await.is_err() {
                break;
            }
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
