use persistentstream::channel;
use bencher::{Bencher, black_box, benchmark_group, benchmark_main};
use tokio::runtime::{Builder, Runtime};
use tempdir::TempDir;
use rand::{distributions::Uniform, Rng};

fn send_and_receive(bench: &mut Bencher) {
    let backup = TempDir::new("/tmp/persist").unwrap();
    let mut runtime = rt();

    bench.iter(|| {
        black_box(runtime.block_on(async {
            let (mut tx, _rx) = channel(&backup.path(), 10, 10 * 1024, 10).unwrap();
            let data = generate_data(1024);

            for _i in 0..1000 {
                tx.send(data.clone()).await.unwrap();
            }
        }))
    });
}

fn generate_data(size: usize) -> Vec<u8> {
    let range = Uniform::from(0..255);
    rand::thread_rng().sample_iter(&range).take(size).collect()
}

fn rt() -> Runtime {
    Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap()
}

benchmark_group!(benches, send_and_receive);
benchmark_main!(benches);
