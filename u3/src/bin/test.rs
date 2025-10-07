#[tokio::main]
async fn main() {
    let t = None;
    tokio::select! {
        a = async { g(t.unwrap()).await }, if t.is_some() => {
            println!("{a}");
        }
        else => {}
    }

}

async fn g(n: u32) -> u32 {
    n+1
}
