use persistentstream::{channel, get_file_ids};

use tempdir::TempDir;
use tokio::task;
use tokio::time;
use tokio::time::Duration;

use rand::{distributions::Uniform, Rng};


fn generate_data(size: usize) -> Vec<u8> {
    let range = Uniform::from(0..255);
    rand::thread_rng().sample_iter(&range).take(size).collect()
}

#[tokio::test]
async fn inmemeory_send_and_receive() {
    let backup = TempDir::new("/tmp/persist").unwrap();
    let (mut tx, mut rx) = channel(&backup.path(), 100, 10 * 1024, 10).unwrap();

    task::spawn(async move {
        for i in 0..100 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.send(data).await.unwrap();
        }
        
        time::delay_for(Duration::from_secs(5)).await;
    });

    for i in 0..100 {
        let data = rx.recv().await.unwrap();
        assert_eq!(data[0], i);
    }
}

#[tokio::test]
async fn inmemory_and_disk_data_amounts_to_correct_number_of_backlog_files() {
    let backup = TempDir::new("/tmp/persist").unwrap();
    let (mut tx, _rx) = channel(&backup.path(), 10, 10 * 1024, 10).unwrap();

    for i in 0..100 {
        let mut data = generate_data(1024);
        data[0] = i;
        tx.send(data).await.unwrap();
    }

    let files = get_file_ids(&backup.path()).unwrap();
    assert_eq!(9, files.len());
}

#[tokio::test]
async fn sync_picks_data_from_the_disk_correctly() {
    let backup = TempDir::new("/tmp/persist").unwrap();
    let (mut tx, mut rx) = channel(&backup.path(), 10, 10 * 1024, 10).unwrap();

    // creates n files on the disk (the current sizes and count doesn't trigger backlog deletes)
    task::spawn(async move {
        for i in 0..100 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.send(data).await.unwrap();
        }

        time::delay_for(Duration::from_secs(5)).await;

        loop {
            tx.sync().await.unwrap();
            time::delay_for(Duration::from_secs(1)).await;
        }
    });

    time::delay_for(Duration::from_secs(5)).await;
    for i in 0..100 {
        let data = rx.recv().await.unwrap();
        assert_eq!(i, data[0]);
    }
}

#[tokio::test]
async fn receiver_picks_data_correctly_after_a_few_backlog_deletions() {
    let backup = TempDir::new("/tmp/persist").unwrap();
    let (mut tx, mut rx) = channel(&backup.path(), 10, 10 * 1024, 10).unwrap();

    // Total files 0..=19
    // After applying retention rule 10..=19 with data from 100..199
    for i in 0..200 {
        let mut data = generate_data(1024);
        data[0] = i;
        tx.send(data).await.unwrap();
    }

    // provides kickstart to read from disk when tx doesn't have any data to send
    task::spawn(async move {
        loop {
            tx.sync().await.unwrap();
            time::delay_for(Duration::from_secs(1)).await;
        }
    });

    let files = get_file_ids(&backup.path()).unwrap();
    assert_eq!(10, files.len());

    // read in memory data
    for i in 0..=10 {
        let data = rx.recv().await.unwrap();
        assert_eq!(i, data[0]);
    }

    // read the remaining non deleted data in the backlog
    for i in 101..200 {
        dbg!(i);
        let data = rx.recv().await.unwrap();
        assert_eq!(i, data[0]);
    }
}


#[tokio::test]
async fn intermittent_delays_in_the_receiver_should_be_handled_correctly() {
    let backup = TempDir::new("/tmp/persist").unwrap();
    // pretty_env_logger::init();
    // let backup = PathBuf::from("/tmp/persist");
    // Total data generated in the below loop = 255K
    // Maximum possible size on disk = 30 * 10KB. So no file deletions
    let (mut tx, mut rx) = channel(&backup, 10, 10 * 1024, 30).unwrap();

    // creates n files on the disk (the current sizes and count doesn't trigger backlog deletes)
    task::spawn(async move {
        // 10 messages per second. i.e 10KB/s 
        for i in 0..255 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.send(data).await.unwrap();
            time::delay_for(Duration::from_millis(100)).await;
        }


        // provides kickstart to read from disk when tx doesn't have any data to send
        loop {
            tx.sync().await.unwrap();
            time::delay_for(Duration::from_secs(1)).await;
        }
    });

    for i in 0..255 {
        let data = rx.recv().await.unwrap();
        if i % 10 == 0 {
            time::delay_for(Duration::from_secs(2)).await;
        }

        assert_eq!(i, data[0]);
    }
}
