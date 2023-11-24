use std::time::Duration;

use rand::Rng;
use serde_json::json;
use tokio::time::interval;
use uplink::base::{
    bridge::{BridgeTx, Payload},
    clock,
};
use vd_lib::{Car, Gear, HandBrake};

async fn simulate(tx: BridgeTx) {
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
    let mut rng = rand::thread_rng();

    loop {
        car.update();
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

async fn forward_device_shadow(tx: &BridgeTx, car: &Car) {
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
    tx.send_payload(data).await;
}
