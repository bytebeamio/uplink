use std::time::{Duration, Instant, SystemTime};

use crate::base::clock;
use flume::{bounded, Receiver, Sender};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::Serialize;
use serde_json::json;
use tokio::{spawn, time::interval};
use vd_lib::{Car, HandBrake};

use super::{
    data::{DeviceData, Gps},
    Event, Payload,
};

const RADIUS_EARTH: f64 = 6371.0;
const UPDATE_INTERVAL: u64 = 1;

async fn update_gps(device: DeviceData, rx: Receiver<f64>, tx: Sender<Event>) {
    let mut gps_track = GpsTrack::new(device.path.as_slice().to_vec());
    let mut sequence = 0;
    loop {
        let speed = rx.recv_async().await.unwrap();
        let distance_travelled = speed / 3600.0;
        for trace in gps_track.traverse(distance_travelled) {
            let payload = json!(trace);
            sequence += 1;
            let data = Payload {
                stream: "c2c_gps".to_string(),
                sequence,
                timestamp: clock() as u64,
                payload,
            };
            tx.send_async(Event::Data(data)).await.unwrap();
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

struct GpsTrack {
    map: Vec<(f64, Gps)>,
    trace_i: usize,
    distance_travelled: f64,
}

impl GpsTrack {
    fn new(mut trace_list: Vec<Gps>) -> Self {
        let mut traces = trace_list.iter();
        let Some(mut last) = traces.next() else {
            panic!("Not enough traces!");
        };
        let mut map = vec![(0.0, last.clone())];

        for trace in traces {
            let distance = haversine(trace, last);
            map.push((distance, trace.clone()));
            last = trace;
        }
        trace_list.reverse();
        let mut traces = trace_list.iter();
        let Some(mut last) = traces.next() else {
            panic!("Not enough traces!");
        };
        for trace in traces {
            let distance = haversine(trace, last);
            map.push((distance, trace.clone()));
            last = trace;
        }

        Self { map, trace_i: 0, distance_travelled: 0.0 }
    }

    fn traverse(&mut self, distance_travelled: f64) -> Vec<Gps> {
        let mut traces = vec![];

        // include trace 0 only for first set of point
        if self.distance_travelled == 0.0 && self.trace_i == 0 {
            traces.push(self.map[0].1.clone())
        }
        self.distance_travelled += distance_travelled;
        loop {
            // skip trace 0 every other time
            if self.trace_i == 0 {
                self.trace_i += 1;
                continue;
            }

            let next_distance = self.map[self.trace_i].0;

            if self.distance_travelled < next_distance {
                return traces;
            }

            self.distance_travelled -= next_distance;
            traces.push(self.map[self.trace_i].1.clone());
            self.trace_i += 1;
            self.trace_i %= self.map.len();
        }
    }
}

#[derive(Serialize)]
struct Session {
    session_start: u64,
    session_end: u64,
}

pub async fn simulate(device: DeviceData, tx: Sender<Event>) {
    let (speed_tx, speed_rx) = bounded(10);
    spawn(update_gps(device, speed_rx, tx.clone()));
    let mut sequence = 0;

    let mut rng = StdRng::from_entropy();
    let charge = rng.gen_range(0.4..1.0);
    let health = rng.gen_range(0.9..1.0);
    let mut car = Car::new(charge, health);
    car.turn_key(true);
    car.set_handbrake_position(HandBrake::Disengaged);
    car.set_accelerator_position(0.5);

    let mut interval = interval(Duration::from_secs(UPDATE_INTERVAL));
    let mut charging = None;
    let mut stopped = None;
    let mut session_start = Some(clock());
    let mut stopping = false;

    loop {
        car.update();
        speed_tx.send_async(car.speed()).await.unwrap();
        sequence += 1;
        forward_device_shadow(&tx, &car, sequence).await;
        interval.tick().await;

        // Turn off for very infrequently
        if car.speed() == 0.0 && car.soc() > 0.25 && stopping {
            stopping = false;
            car.set_handbrake_position(HandBrake::Full);
            car.set_brake_position(0.0);
            car.turn_key(false);

            stopped = Some(
                // Time during which car is stationary: between 5-100 minutes
                Instant::now() + Duration::from_secs_f32(300.0 + 0.0 * rng.gen_range(0.0..95.0)),
            );
            if let Some(start) = session_start.take() {
                forward_session(&tx, start, sequence).await
            }
        }

        if let Some(till) = stopped {
            if till < Instant::now() {
                stopped.take();
                car.turn_key(true);
                session_start = Some(clock());
            }
            continue;
        }

        // Stop for charging when low battery or very infrequently to end a session
        if (car.soc() < 0.25 || rng.gen_bool(0.0005)) && car.speed() != 0.0 {
            car.set_brake_position(rng.gen_range(0.3..0.9));
            stopping = true;
            continue;
        }
        // Start charging
        if car.soc() < 0.25 && car.speed() == 0.0 && charging.is_none() {
            car.set_handbrake_position(HandBrake::Full);
            car.set_brake_position(0.0);
            if rng.gen_bool(0.5) {
                car.turn_key(false);
            }
            car.set_status("Charging");
            charging = Some(
                // Time during which car is stationary at the charging point: between 7.5-17.5 minutes
                Instant::now() + Duration::from_secs_f32(300.0 + 60.0 * rng.gen_range(2.5..12.5)),
            );
        }

        if let Some(till) = charging {
            if till < Instant::now() {
                charging.take();
                if !car.ignition() {
                    car.turn_key(true);
                    car.set_status("Running");
                }
            }
            car.charge(rng.gen_range(0.01..0.05));
            continue;
        }

        // very few times, press the brake to slow down, else remove
        if rng.gen_bool(0.05) || car.brake_position() > 0.5 {
            car.set_brake_position(rng.gen_range(0.3..1.0));
            continue;
        } else {
            car.set_brake_position(0.0);
        }

        // even fewer times, engage hand brake to slow down instantly, or else do the opposite
        if rng.gen_bool(0.005) {
            if rng.gen_bool(0.25) || car.hand_brake() == &HandBrake::Half {
                car.set_handbrake_position(HandBrake::Full);
                continue;
            } else {
                car.set_handbrake_position(HandBrake::Half);
            }
        } else if car.hand_brake() != &HandBrake::Disengaged {
            car.set_handbrake_position(
                if rng.gen_bool(0.25) || car.hand_brake() == &HandBrake::Full {
                    HandBrake::Half
                } else {
                    HandBrake::Disengaged
                },
            );
        }

        car.set_accelerator_position(rng.gen_range(0.25..1.0));
    }
}

async fn forward_device_shadow(tx: &Sender<Event>, car: &Car, sequence: u32) {
    let payload = json!({
        "Status": car.get_status(),
        "efficiency": car.distance_travelled() / car.energy_consumed(),
        "distance_travelled_km": car.distance_travelled(),
        "range": ((car.soh() - car.soc()) * 400.0) as u16, // maximum the car can ever go is 400km
        "SoC": (car.soc() * 100.0) as u8,
        "speed": car.speed(),
    });
    let data = Payload {
        stream: "device_shadow".to_string(),
        sequence,
        timestamp: clock() as u64,
        payload,
    };
    tx.send_async(Event::Data(data)).await.unwrap();
}

async fn forward_session(tx: &Sender<Event>, start: u128, sequence: u32) {
    let payload = json!({
        "start_time": start,
        "end_time": SystemTime::UNIX_EPOCH.elapsed().unwrap().as_millis(),
    });
    let data =
        Payload { stream: "sessions".to_string(), sequence, timestamp: clock() as u64, payload };
    tx.send_async(Event::Data(data)).await.unwrap();
}
