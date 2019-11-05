use std::collections::HashMap;
use derive_more::From;

#[derive(Debug, From)]
pub struct Data {
    data: HashMap<String, String>,
}

pub mod simulator;