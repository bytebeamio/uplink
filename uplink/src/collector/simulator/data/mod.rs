mod default;
pub mod driver;

pub use default::{Bms, DeviceData, DeviceShadow, Gps, Imu, Motor, PeripheralState};
use flume::Sender;
use tokio::spawn;

use crate::base::{SimulatorConfig, SimulatorProfile};

use self::driver::ElectricVehicle;

use super::Event;

pub fn spawn_data_simulators(config: SimulatorConfig, device: DeviceData, tx: Sender<Event>) {
    match config.profile {
        SimulatorProfile::Default => {
            spawn(Gps::simulate(tx.clone(), device));
            spawn(Bms::simulate(tx.clone()));
            spawn(Imu::simulate(tx.clone()));
            spawn(Motor::simulate(tx.clone()));
            spawn(PeripheralState::simulate(tx.clone()));
            spawn(DeviceShadow::simulate(tx));
        }
        SimulatorProfile::Driver => {
            spawn(ElectricVehicle::simulate(tx, device));
        }
    }
}
