use std::time::Duration;

use flume::{bounded, Receiver, Sender};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::json;
use tokio::{spawn, time::interval};
use uplink::base::clock;
use vd_lib::{Car, Gear, HandBrake};

use crate::{
    basic::{DeviceData, Gps},
    Payload,
};

const RADIUS_EARTH: f64 = 6371.0;

async fn update_gps(device: DeviceData, rx: Receiver<f64>, tx: Sender<Payload>) {
    let mut gps_track = GpsTrack::new(device.path.as_slice().to_vec());
    let mut sequence = 0;
    loop {
        let speed = rx.recv_async().await.unwrap();
        let distance_travelled = speed / 3600.0;
        for trace in gps_track.traverse(distance_travelled) {
            let payload = json!(trace);
            sequence += 1;
            let data =
                Payload { stream: "gps".to_string(), sequence, timestamp: clock() as u64, payload };
            tx.send_async(data).await.unwrap();
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

pub async fn simulate(device: DeviceData, tx: Sender<Payload>) {
    let (speed_tx, speed_rx) = bounded(10);
    spawn(update_gps(device, speed_rx, tx.clone()));
    let mut car = Car::default();
    car.set_handbrake_position(HandBrake::Disengaged);
    car.set_clutch_position(1.0);
    car.shift_gear(Gear::First);
    car.set_clutch_position(0.5);
    car.set_accelerator_position(0.5);
    car.update();
    forward_device_shadow(&tx, &car).await;
    car.set_clutch_position(0.0);

    let mut interval = interval(Duration::from_secs(1));
    let mut rng = StdRng::from_entropy();

    loop {
        car.update();
        speed_tx.send_async(car.speed()).await.unwrap();
        forward_device_shadow(&tx, &car).await;
        interval.tick().await;

        if rng.gen_bool(0.05) && car.rpm() > 2500 || car.rpm() > 3500 || car.rpm() < 1250 {
            shift_gears(&mut car, rng.gen_range(0.25..1.0));
        } else {
            car.set_clutch_position(0.0);
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

fn shift_gears(car: &mut Car, clutch_position: f64) {
    let clutch_gear_combo = |car: &mut Car, gear| {
        car.set_clutch_position(clutch_position);
        car.shift_gear(gear);
    };
    match car.gear() {
        Gear::Reverse => clutch_gear_combo(car, Gear::Neutral),
        Gear::Neutral => {
            if car.clutch_position() > 0.5 {
                clutch_gear_combo(car, Gear::First)
            }
        }
        Gear::First => {
            if car.rpm() > 2500 && car.speed() > 10.0 {
                clutch_gear_combo(car, Gear::Second)
            }
        }
        Gear::Second => match car.speed() as u8 {
            0..=10 => clutch_gear_combo(car, Gear::First),
            s if s > 25 && car.rpm() > 3000 => clutch_gear_combo(car, Gear::Third),
            _ => {}
        },
        Gear::Third => match car.speed() as u8 {
            0..=10 => clutch_gear_combo(car, Gear::First),
            11..=20 => clutch_gear_combo(car, Gear::Second),
            s if s > 50 && car.rpm() > 3500 => clutch_gear_combo(car, Gear::Fourth),
            _ => {}
        },
        Gear::Fourth => match car.speed() as u8 {
            0..=10 => clutch_gear_combo(car, Gear::First),
            11..=20 => clutch_gear_combo(car, Gear::Second),
            21..=40 => clutch_gear_combo(car, Gear::Third),
            s if s > 80 && car.rpm() > 4000 => clutch_gear_combo(car, Gear::Fifth),
            _ => {}
        },
        Gear::Fifth => match car.speed() as u8 {
            0..=10 => clutch_gear_combo(car, Gear::First),
            11..=20 => clutch_gear_combo(car, Gear::Second),
            21..=40 => clutch_gear_combo(car, Gear::Third),
            41..=70 => clutch_gear_combo(car, Gear::Fourth),
            _ => {}
        },
    }
}

async fn forward_device_shadow(tx: &Sender<Payload>, car: &Car) {
    let payload = json!({
        "speed": car.speed(),
        "gear": car.gear(),
        "rpm": car.rpm(),
        "accelerator": car.accelerator_position(),
        "brake": car.brake_position(),
        "clutch": car.clutch_position(),
        "hand_brake": car.hand_brake()
    });
    let data = Payload {
        stream: "device_shadow".to_string(),
        sequence: 0,
        timestamp: clock() as u64,
        payload,
    };
    tx.send_async(data).await.unwrap();
}
