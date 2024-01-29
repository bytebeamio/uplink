use std::time::{Duration, Instant, SystemTime};

use crate::base::clock;
use flume::{bounded, Receiver, Sender};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::Serialize;
use serde_json::json;
use tokio::{spawn, time::interval};
use vd_lib::{Ev, HandBrake};

use super::{
    data::{DeviceData, Gps},
    Event, Payload,
};

const TOTAL_CAPACITY: f64 = 85.0;
const MAX_RANGE: f64 = 105.0;
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
    let cycles = rng.gen_range(20..100) as usize;
    let mut ev = Ev::new(charge, health, cycles);
    ev.turn_key(true);
    ev.set_handbrake_position(HandBrake::Disengaged);
    ev.set_accelerator_position(0.5);
    ev.start_running();

    let mut interval = interval(Duration::from_secs(UPDATE_INTERVAL));
    let mut charging = None;
    let mut stopped = None;
    let mut session_start = Some(clock());
    let mut stopping = false;

    loop {
        ev.update(&mut rng);
        speed_tx.send_async(ev.speed()).await.unwrap();
        sequence += 1;
        forward_device_shadow(&tx, &ev, sequence).await;
        forward_cell_voltages(&tx, &ev, sequence).await;
        forward_fet_status(&tx, &ev, sequence).await;
        interval.tick().await;

        // Turn off for very infrequently
        if ev.speed() == 0.0 && ev.soc() > 0.25 && stopping {
            stopping = false;
            ev.stop();
            ev.set_handbrake_position(HandBrake::Full);
            ev.set_brake_position(0.0);
            ev.turn_key(false);

            stopped = Some(
                // Time during which ev is stationary: between 5-100 minutes
                Instant::now() + Duration::from_secs_f32(300.0 + 0.0 * rng.gen_range(0.0..95.0)),
            );
            if let Some(start) = session_start.take() {
                forward_session(&tx, start, sequence).await
            }
        }

        if let Some(till) = stopped {
            if till < Instant::now() {
                stopped.take();
                ev.turn_key(true);
                session_start = Some(clock());
            }
            continue;
        }

        // Stop for charging when low battery or very infrequently to end a session
        if (ev.soc() < 0.25 || rng.gen_bool(0.0005)) && ev.speed() != 0.0 {
            ev.set_brake_position(rng.gen_range(0.3..0.9));
            stopping = true;
            continue;
        }
        // Start charging
        if ev.soc() < 0.25 && ev.speed() == 0.0 && charging.is_none() {
            ev.set_handbrake_position(HandBrake::Full);
            ev.set_brake_position(0.0);
            if rng.gen_bool(0.5) {
                ev.turn_key(false);
            }
            ev.start_charging();
            charging = Some(
                // Time during which ev is stationary at the charging point: between 7.5-17.5 minutes
                Instant::now() + Duration::from_secs_f32(300.0 + 60.0 * rng.gen_range(2.5..12.5)),
            );
        }

        if let Some(till) = charging {
            if till < Instant::now() {
                charging.take();
                if !ev.ignition() {
                    ev.turn_key(true);
                    ev.start_running();
                }
            }
            ev.charge(rng.gen_range(0.01..0.05));
            continue;
        }

        // very few times, press the brake to slow down, else remove
        if rng.gen_bool(0.05) || ev.brake_position() > 0.5 {
            ev.set_brake_position(rng.gen_range(0.3..1.0));
            continue;
        } else {
            ev.set_brake_position(0.0);
        }

        // even fewer times, engage hand brake to slow down instantly, or else do the opposite
        if rng.gen_bool(0.005) {
            if rng.gen_bool(0.25) || ev.hand_brake() == &HandBrake::Half {
                ev.set_handbrake_position(HandBrake::Full);
                continue;
            } else {
                ev.set_handbrake_position(HandBrake::Half);
            }
        } else if ev.hand_brake() != &HandBrake::Disengaged {
            ev.set_handbrake_position(
                if rng.gen_bool(0.25) || ev.hand_brake() == &HandBrake::Full {
                    HandBrake::Half
                } else {
                    HandBrake::Disengaged
                },
            );
        }

        ev.set_accelerator_position(rng.gen_range(0.25..1.0));
    }
}

async fn forward_device_shadow(tx: &Sender<Event>, ev: &Ev, sequence: u32) {
    let payload = json!({
        "Status": ev.get_status(),
        "efficiency": ev.distance_travelled() / ev.energy_consumed(),
        "distance_travelled_km": ev.distance_travelled(),
        "range": ((ev.soh() - ev.soc()) * MAX_RANGE) as u16, // maximum the ev can ever go is 400km
        "SoC": (ev.soc() * 100.0) as u8,
        "speed": ev.speed(),
        "total_capacity": TOTAL_CAPACITY,
        "discharge_cycles": ev.get_discharge_cycles(),
        "remaining_capacity": ev.soc() * TOTAL_CAPACITY,
        "current": ev.get_current(),
        "voltage": ev.get_voltage(),
        "production_date": "26 January 2024",
        "name": "L126012024",
    });
    let data = Payload {
        stream: "device_shadow".to_string(),
        sequence,
        timestamp: clock() as u64,
        payload,
    };
    tx.send_async(Event::Data(data)).await.unwrap();
}

async fn forward_cell_voltages(tx: &Sender<Event>, ev: &Ev, sequence: u32) {
    for (i, cell) in ev.get_cell_stats().enumerate() {
        let payload = serde_json::to_value(cell).unwrap();
        let data = Payload {
            stream: format!("battery_cell_{i}"),
            sequence,
            timestamp: clock() as u64,
            payload,
        };

        tx.send_async(Event::Data(data)).await.unwrap();
    }
}

async fn forward_fet_status(tx: &Sender<Event>, ev: &Ev, sequence: u32) {
    let payload = serde_json::to_value(ev.get_fet_control_status()).unwrap();
    let data = Payload {
        stream: format!("fet_control_status"),
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
