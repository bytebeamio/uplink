use futures_util::stream::Stream;
use rumq_client::{self, MqttEventLoop, MqttOptions, Notification, Request};
use tokio::sync::mpsc::{channel, Sender};

pub struct Actions<'actions> {
    mqtt_tx: Sender<Request>,
    eventloop: Box<MqttEventLoop>,
    stream: Option<&'actions dyn Stream<Item = Notification>>,
}

impl<'actions> Actions<'actions> {
    pub fn new() -> Actions<'actions> {
        let options = mqttoptions();
        let (requests_tx, requests_rx) = channel(1);

        let eventloop = rumq_client::eventloop(options, requests_rx);
        let eventloop = Box::new(eventloop);

        let mut actions = Actions {
            mqtt_tx: requests_tx,
            eventloop,
            stream: None,
        };

        let eventloop = &mut actions.eventloop;
        let stream = eventloop.stream();
        actions.stream = Some(&stream);
        actions
    }
}

fn mqttoptions() -> MqttOptions {
    let mut mqttoptions = MqttOptions::new("device-1", "localhost", 1883);
    mqttoptions.set_keep_alive(30);

    mqttoptions
}
