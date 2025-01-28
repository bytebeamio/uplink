use flume::{bounded, Receiver, Sender};
use log::{info, warn};
use tokio::select;

use super::Payload;
use crate::base::actions::Cancellation;
use crate::uplink_config::{ActionRoute, Config};
use crate::utils::LimitedArrayMap;
use crate::{Action, ActionCallback, ActionResponse};
use std::fmt::{Display, Formatter};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use std::path::Path;
use anyhow::Context;

#[derive(Debug)]
pub enum Error {
    DuplicateActionRoutes { action_name: String },
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl std::error::Error for Error {}

pub struct ActionsBridge {
    /// Full configuration
    config: Arc<Config>,
    /// Tx handle to give to apps
    status_tx: Sender<ActionResponse>,
    /// Rx to receive action status from apps
    status_rx: Receiver<ActionResponse>,
    /// Actions incoming from backend
    actions_rx: Receiver<Action>,
    /// action responses going to backend
    data_tx: Sender<Payload>,
    /// Apps registered with the bridge
    /// NOTE: Sometimes action_routes could overlap, the latest route
    /// to be registered will be used in such a circumstance.
    action_routes: HashMap<String, Sender<Action>>,
    /// Action redirections
    action_redirections: HashMap<String, String>,
    /// A mapping from action ids to their routes
    /// Ideally we shouldn't have to maintain this state but fixing this would require
    /// changing the uplink config format by removing the redirection clause
    /// We only ever redirect from downloader to other apps
    /// There is no point in forcing users to configure this behavior
    /// Downloading can be an inner feature of tcpapps, installer, and script runner
    ///
    /// Right now this will malfunction if:
    /// * An action is triggered
    /// * Device receives 64 actions after it
    /// * The user tries cancelling the first action
    /// * The cancel action will fail
    actions_routing_cache: LimitedArrayMap<String, String>,
    /// used if uplink is used as a library
    actions_callback: Option<ActionCallback>,
}

impl ActionsBridge {
    pub fn new(
        config: Arc<Config>,
        actions_rx: Receiver<Action>,
        data_tx: Sender<Payload>,
        actions_callback: Option<ActionCallback>,
    ) -> Self {
        let (status_tx, status_rx) = bounded(10);
        let action_redirections = config.action_redirections.clone();

        Self {
            status_tx,
            status_rx,
            actions_routing_cache: Self::load_actions_routing_cache(config.persistence_path.as_path()),
            config,
            actions_rx,
            data_tx,
            action_routes: HashMap::with_capacity(16),
            action_redirections,
            actions_callback,
        }
    }

    fn load_actions_routing_cache(persistence: &Path) -> LimitedArrayMap<String, String> {
        let save_file = persistence.join("actions_routing_cache.json");
        if std::fs::metadata(&save_file).is_err() {
            return LimitedArrayMap::new(32);
        }
        let mut result = std::fs::read(&save_file)
            .context("")
            .and_then(|data| serde_json::from_slice::<LimitedArrayMap<String, String>>(data.as_slice())
                .context("actions_routing_cache.json has invalid data")).unwrap_or_else(|e| {
            warn!("Couldn't read actions_routing_cache.json: {e}, resetting to default");
            if let Err(e) = std::fs::remove_file(&save_file) {
                warn!("Couldn't remove a file in persistence directory: {e}. Does the uplink process have right permissions?");
            }
            LimitedArrayMap::new(32)
        });
        result.map.reserve(32);
        result
    }

    async fn persist_actions_routing_cache(&self) {
        let save_file = self.config.persistence_path.join("actions_routing_cache.json");
        if let Err(e) = std::fs::write(
            &save_file,
            serde_json::to_vec(&self.actions_routing_cache)
                // this unwrap is safe as per serde_json::to_vec_pretty requirements
                .unwrap(),
        ) {
            log::error!("Couldn't write to a file in persistence directory: {e}. Does the uplink process have right permissions?");
        }
    }

    pub fn register_action_route(
        &mut self,
        ActionRoute { name, .. }: ActionRoute,
        actions_tx: Sender<Action>,
    ) -> Result<(), Error> {
        if self.action_routes.insert(name.clone(), actions_tx).is_some() {
            return Err(Error::DuplicateActionRoutes { action_name: name });
        }

        Ok(())
    }

    pub fn register_action_routes<R: Into<ActionRoute>, V: IntoIterator<Item = R>>(
        &mut self,
        routes: V,
        actions_tx: Sender<Action>,
    ) -> Result<(), Error> {
        for route in routes {
            self.register_action_route(route.into(), actions_tx.clone())?;
        }

        Ok(())
    }

    /// Handle to send action status messages from connected application
    pub fn status_tx(&self) -> Sender<ActionResponse> {
        self.status_tx.clone()
    }

    pub async fn start(&mut self) -> Result<(), String> {
        loop {
            select! {
                action = self.actions_rx.recv_async() => {
                    let action = action.map_err(|e| format!("Encountered error when receiving action from broker: {e:?}"))?;

                    // Reactlabs setup processes logs generated by uplink
                    info!("Received action: {:?}", action);
                    self.forward_action_response(
                        ActionResponse::progress(action.action_id.as_str(), "Received", 0)
                    ).await;

                    self.handle_incoming_action(action).await;
                }

                response = self.status_rx.recv_async() => {
                    let response = response.map_err(|e| format!("Encountered error when receiving status from a collector: {e:?}"))?;
                    self.forward_action_response(response).await;
                }
            }
        }
    }

    async fn handle_incoming_action(&mut self, action: Action) {
        Box::pin(async move {
            if action.name == "cancel_action" {
                match serde_json::from_str::<Cancellation>(action.payload.as_str()) {
                    Ok(payload) => {
                        if let Some(route_id) = self.actions_routing_cache.get(&payload.action_id).map(|e| e.clone()) {
                            self.try_route_action(route_id.as_str(), action).await;
                        } else {
                            self.forward_action_response(
                                ActionResponse::failure(action.action_id.as_str(), "Cancellation request received for action currently not in execution!")
                            ).await;
                        }
                    }
                    Err(_) => {
                        log::error!("Invalid cancel action payload: {:#?}", action.payload);
                        self.forward_action_response(
                            ActionResponse::failure(action.action_id.as_str(), format!("Invalid cancel action payload: {:#?}", action.payload))
                        ).await;
                    }
                }
            } else {
                self.try_route_action(action.name.as_str(), action.clone()).await;
            }
        }).await;
    }

    /// Handle received actions
    async fn try_route_action(&mut self, route_id: &str, action: Action) {
        match self.action_routes.get(route_id) {
            Some(route) => {
                if let Err(e) = route.try_send(action.clone()) {
                    log::error!("Could not forward action to collector: {e}");
                    self.forward_action_response(ActionResponse::failure(action.action_id.as_str(), format!("Could not forward action to collector: {e}"))).await;
                } else {
                    if let Some((action_id, app_name)) = self.actions_routing_cache.set(action.action_id.to_owned(), route_id.to_owned()) {
                        log::info!("Dropping routing info about action_id: {action_id} executed on app: {app_name}");
                    }
                    self.persist_actions_routing_cache().await;
                }
            }
            None => {
                match &self.actions_callback {
                    Some(cb) => {
                        // forgive me for the harm I've caused this world
                        // none may atone for my actions but me
                        // and only in me shall the stain live on
                        // all I can be is sorry and that is all I am
                        cb(action);
                    }
                    None => {
                        self.forward_action_response(ActionResponse::failure(action.action_id.as_str(), format!("Uplink isn't configured to handle actions of type {route_id}"))).await;
                    }
                }
            }
        }
    }

    async fn forward_action_response(&mut self, response: ActionResponse) {
        info!("Action response = {:?}", response);

        let _ = self.data_tx.send_async(response.to_payload()).await;

        if let Some(mut redirected_action) = response.done_response {
            if let Some(target_action) = self.action_redirections.get(redirected_action.name.as_str()) {
                target_action.clone_into(&mut redirected_action.name);
                self.handle_incoming_action(redirected_action).await;
            }
        }
    }
}
