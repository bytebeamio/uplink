use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use crate::base::clock;

#[derive(Debug, Serialize, Clone)]
pub struct TunshellMetrics {
    pub timestamp: u128,
    pub sequence: u32,
    /// Total count of sessions
    pub total_sessions: u32,
    /// Actions that triggered currently running sessions
    pub current_actions: HashSet<String>,
    /// Count of currently running session
    pub current_sessions: u32,
    #[serde(skip_serializing)]
    pub session_start_times: HashMap<String, Instant>,
    /// Shortest session duration in seconds
    pub shortest_session: u64,
    /// Longest session duration in seconds
    pub longest_session: u64,
    /// Actions that triggered a tunshell session to start
    pub actions: Vec<String>,
    /// Errors faced while running an tunshell session
    pub errors: Vec<String>,
}

impl TunshellMetrics {
    pub fn new() -> Self {
        Self {
            timestamp: clock(),
            sequence: 1,
            total_sessions: 0,
            current_sessions: 0,
            current_actions: HashSet::new(),
            session_start_times: HashMap::new(),
            shortest_session: 0,
            longest_session: 0,
            actions: vec![],
            errors: vec![],
        }
    }

    pub fn new_session(&mut self, action_id: String) {
        self.current_actions.insert(action_id.to_owned());
        self.session_start_times.insert(action_id.to_owned(), Instant::now());
        self.actions.push(action_id);
        self.total_sessions += 1;
        self.current_sessions += 1;
    }

    pub fn remove_session(&mut self, action_id: String) {
        self.current_sessions -= 1;
        self.timestamp = clock();

        let time_elapsed = self
            .session_start_times
            .remove(&action_id)
            .expect(&format!("Unexpected action_id: {action_id}"))
            .elapsed()
            .as_secs();
        self.current_actions.remove(&action_id);

        self.longest_session = self.longest_session.max(time_elapsed);
        self.shortest_session = self.shortest_session.min(time_elapsed);
    }

    pub fn add_error(&mut self, error: &String) {
        self.errors.push(error.to_string());
    }

    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;

        self.actions.clear();
        self.errors.clear();
    }
}
