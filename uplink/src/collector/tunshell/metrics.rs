use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use super::Error as TunshellError;
use crate::base::clock;

#[derive(Debug, Serialize, Clone, Default)]
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
    /// Shortest session duration (seconds)
    pub shortest_session: Option<f64>,
    /// Longest session duration (seconds)
    pub longest_session: Option<f64>,
    /// Actions that triggered a tunshell session since last metrics push
    pub actions: Vec<String>,
    /// Errors faced since last metrics push
    pub errors: Vec<String>,
}

impl TunshellMetrics {
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
            .unwrap_or_else(|| panic!("Unexpected action_id: {action_id}"))
            .elapsed()
            .as_secs_f64();
        self.current_actions.remove(&action_id);

        if let Some(longest_session) = &mut self.longest_session {
            *longest_session = longest_session.max(time_elapsed);
        } else {
            self.longest_session = Some(time_elapsed)
        }
        if let Some(shortest_session) = &mut self.shortest_session {
            *shortest_session = shortest_session.min(time_elapsed);
        } else {
            self.longest_session = Some(time_elapsed)
        }
    }

    pub fn add_error(&mut self, error: &TunshellError) {
        self.errors.push(error.to_string());
    }

    pub fn capture(&mut self) -> Self {
        self.timestamp = clock();
        let metrics = self.clone();

        self.sequence += 1;
        self.longest_session.take();
        self.shortest_session.take();
        self.actions.clear();
        self.errors.clear();

        metrics
    }
}
