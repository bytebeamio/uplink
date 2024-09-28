use std::fs::metadata;

use sqlx::{migrate::MigrateDatabase, Sqlite};
use tokio::runtime::Runtime;
use vergen::{vergen, Config};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate the default 'cargo:' instruction output
    vergen(Config::default())?;

    let path = "../.persistence/events.db";
    if metadata(path).is_err() {
        Runtime::new().unwrap().block_on(Sqlite::create_database(path)).unwrap();
    }
    Ok(())
}
