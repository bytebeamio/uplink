use std::{slice::Iter, time::Duration};

use flume::{SendError, Sender};
use log::error;
use rand::random;
use serde::Serialize;
use serde_json::json;
use tokio::time::sleep;

use crate::base::{bridge::Payload, clock};

use super::{DeviceData, Event, Gps};

// Constants
const RADIUS_EARTH: f64 = 6371.0;
const BATTERY_CAPACITY: f64 = 3.7;
const HEALTH_PENALTY: f64 = 0.000002;
const ENERGY_CONSUMPTION: f64 = 5.0;
const CHARGING_RATE: f64 = 22.0;
const WHEEL_CIRCUMFERENCE: f64 = 32.0;
const TIME_PERIOD: f64 = 0.005555; // 20 seconds in hrs is 0.005555

#[derive(Debug, Serialize)]
pub struct ElectricVehicle {
    #[serde(skip)]
    tx: Sender<Event>,
    #[serde(skip)]
    sequence: u32,
    #[serde(skip)]
    location: Gps,
    #[serde(rename = "SOC")]
    soc: f64,
    #[serde(rename = "SOH")]
    soh: f64,
    energy_left: f64,
    #[serde(rename = "Status")]
    state: String,
    ignition: bool,
    acceleration: f64,
    speed: f64,
    distance_travelled: f64,
    rpm: f64,
}

impl ElectricVehicle {
    fn new(tx: Sender<Event>, location: Gps) -> Self {
        Self {
            tx,
            location,
            sequence: 0,
            soc: 1.0,
            soh: 1.0,
            energy_left: BATTERY_CAPACITY,
            state: "Stopped".to_string(),
            ignition: true,
            acceleration: 0.0,
            speed: 0.0,
            distance_travelled: 0.0,
            rpm: 0.0,
        }
    }

    fn drive(&mut self, distance: f64) {
        self.distance_travelled += distance;
        self.energy_left -= distance * ENERGY_CONSUMPTION * TIME_PERIOD;
        let speed = distance / TIME_PERIOD;
        self.acceleration = (speed - self.speed) / (TIME_PERIOD * 12960.0);
        self.speed = speed;
        self.rpm = self.speed / (WHEEL_CIRCUMFERENCE * 0.001885);

        self.soc = self.energy_left / BATTERY_CAPACITY;

        if self.soc < 0.2 {
            self.soh -= HEALTH_PENALTY; // penalty for driving on low battery
        }
    }

    fn idle(&mut self) {
        self.rpm = 0.0;
        self.acceleration = -self.speed / (TIME_PERIOD * 12960.0);
        self.speed = 0.0;

        // Some energy is lost when ignition is turned on and system is idling
        if self.ignition {
            self.energy_left -= ENERGY_CONSUMPTION * TIME_PERIOD * 0.001;
        }
    }

    fn stop(&mut self) {
        self.ignition = false;
        self.idle();
    }

    fn charge(&mut self) {
        self.stop();

        self.energy_left += CHARGING_RATE * TIME_PERIOD; // Assuming 1 time step = 5 seconds
        if self.energy_left > BATTERY_CAPACITY {
            self.energy_left = BATTERY_CAPACITY;
            self.soh -= HEALTH_PENALTY; // penalty for overcharging on battery health
        }
        self.soc = self.energy_left / BATTERY_CAPACITY
    }

    fn update_state(&mut self) {
        if self.speed > 0.0 {
            self.state = "Running".to_string()
        } else if self.ignition {
            self.state = "Idling".to_string()
        } else if self.soc < 0.2 && self.soc > 0.0 {
            self.state = "Charging".to_string()
        } else {
            self.state = "Stopped".to_string()
        }
    }

    // Push data point updates and sleep
    async fn update_and_sleep(&mut self) -> Result<(), SendError<Event>> {
        self.sequence += 1;
        self.update_state();
        self.tx
            .send_async(Event::Data(Payload {
                timestamp: clock() as u64,
                stream: "gps".to_string(),
                sequence: self.sequence,
                payload: json!(self.location),
            }))
            .await?;

        self.tx
            .send_async(Event::Data(Payload {
                timestamp: clock() as u64,
                stream: "device_shadow".to_string(),
                sequence: self.sequence,
                payload: json!(self),
            }))
            .await?;

        sleep(Duration::from_secs_f64(3600.0 * TIME_PERIOD)).await;

        Ok(())
    }

    async fn trace_map(&mut self, mut map: Iter<'_, Gps>) {
        // We don't care about the first trace, since == self.location
        _ = map.next();

        while let Some(mut trace) = map.next() {
            let mut distance = haversine(trace, &self.location);
            self.location = *trace;
            // Randomly speed up the the vehicle to 2x
            if random::<f64>() < 0.25 {
                trace = match map.next() {
                    Some(t) => t,
                    _ => return,
                };
                distance += haversine(trace, &self.location);
                self.location = *trace;
            }

            self.drive(distance);
            if let Err(e) = self.update_and_sleep().await {
                error!("{e}");
                return;
            }
            while random::<f64>() < 0.2 {
                // Randomly turn off the vehicle, else idle
                if random::<f64>() > 0.5 {
                    self.stop();
                } else {
                    self.idle()
                }
                if let Err(e) = self.update_and_sleep().await {
                    error!("{e}");
                    return;
                }
            }
        }
    }

    pub async fn simulate(tx: Sender<Event>, device: DeviceData) {
        let mut map = device.path;
        let mut ev = Self::new(tx, *map.first().unwrap());

        // Follow the map, reverse and return to starting point, repeat
        loop {
            ev.trace_map(map.iter()).await;

            // If battery is below 85%, charge at the end of trip
            while ev.soc < 0.85 {
                ev.charge();
                if let Err(e) = ev.update_and_sleep().await {
                    error!("{e}");
                }
            }

            map.reverse();
        }
    }
}

// Calculates the distance between two points on the globe
fn haversine(current: &Gps, last: &Gps) -> f64 {
    let delta_lat = (last.latitude - current.latitude).to_radians();
    let delta_lon = (last.longitude - current.longitude).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + current.latitude.to_radians().cos()
            * last.latitude.to_radians().cos()
            * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    RADIUS_EARTH * c
}
