# uplink

[![Rust][workflow-badge]][workflow] [![@bytebeamio][twitter-badge]][twitter]

<img align="right" src="docs/logo.png" height="150px" alt="the uplink logo">

uplink is a *utility/library* written in Rust for connecting devices running linux with an MQTT backend. The primary target for uplink is the [**Bytebeam**][bytebeam] platform, however uplink can also be used with any broker supporting MQTT 3.1.

### Features

- JSON formatted data push to cloud.
- Receives commands from the cloud, executes them and updates progress of execution.
- Auto downloads updates from the cloud to perform OTA.
- Supports TLS.

### Build and Install

Build and install with [Cargo][crates.io]:

```sh
cargo install uplink # NOTE: This should work in the future
```

#### Build and run from source:

```sh
git clone https://github.com/bytebeamio/uplink.git
cd uplink
cargo run --bin uplink -- -a <device auth json file>
```
#### Build and from source for ARM systems

In case you want to run uplink on an ARM system, follow instruction given below

1. Install arm compilers and linkers

```sh
apt install gcc-9-arm-linux-gnueabihf
ln -s /usr/bin/arm-linux-gnueabihf-gcc-9 /usr/bin/arm-linux-gnueabihf-gcc
```
2. Create `.cargo/config`

```toml
[target.armv7-unknown-linux-gnueabihf]
linker = "arm-linux-gnueabihf-gcc"

[build]
rustflags = ["-C", "rpath"]
```
3. Install necessary architecture target with rustup and build binary 
```sh
rustup target install armv7-unknown-linux-gnueabihf
cargo build --release --target armv7-unknown-linux-gnueabihf
```
4. Retreive executable from `/target/release/uplink` and execute it on target device.

See [releases][releases] for other options.

### Getting Started

- To use uplink, you need to provide it with an authenication file, let us first source it from the Bytebeam platform.
    1. Login to your account on [demo.bytebeam.io](https://demo.bytebeam.io) and ensure that you are in the right organization.
    2. From within the "Device Management" UI, select the "Devices" tab and press on the "Create Device" button.
    3. Press "Submit" and your browser will download the newly created device's authentication file with a JSON format mentioned in the [Secure Communication over TLS section](#secure-communication-over-tls).

```sh
uplink -a certificates.json -vv # use `cargo run --bin uplink --` incase uplink isn't installed
```
### Architecture
uplink acts as an intermediary between the user's applications and the Bytebeam platform. Connecting the applications to the bridge port one can communciate with the platform over MQTT with TLS, accepting JSON structured [Action][action]s and forwarding either JSON formatted data(from applications such as sensing) or [Action Response][action_response]s that report the progress of aforementioned Actions.

<img src="docs/uplink.png" height="150px" alt="uplink architecture">

### Secure Communication over TLS
uplink communicates with the Bytebeam platform over MQTT+TLS, ensuring application data is encrypted in transit. Bytebeam also uses SSL certificates to authenticate connections and identify devices, and uplink expects users to provide it with an auth file with the `-a` flag. This file contains certificate information for the device and the certifying authority in the following JSON format:
```js
{
    "project_id": "xxxx",
    "broker": "example.com",
    "port": 8883,
    "device_id": "yyyy",
    "authentication": {
        "ca_certificate": "...",
        "device_certificate": "...",
        "device_private_key": "..."
    }
}
```

### Testing with netcat

You can test sending JSON data to Bytebeam over uplink with the following command while uplink is active

```sh
nc localhost 5555
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
```

The complete API reference for the uplink library is available within the [library documentation][docs.rs].

### Contributing
Please follow the [code of conduct][coc] while opening issues to report bugs or before you contribute fixes, also do read our [contributor guide][contribute] to get a better idea of what we'd appreciate and what we won't.

[workflow-badge]: https://github.com/bytebeamio/uplink/actions/workflows/rust.yml/badge.svg
[workflow]: https://github.com/bytebeamio/uplink/actions/workflows/rust.yml
[twitter-badge]: https://img.shields.io/twitter/follow/bytebeamio.svg?style=social&label=Follow
[twitter]: https://twitter.com/intent/follow?screen_name=bytebeamio
[bytebeam]: https://bytebeam.io
[rumqtt]: https://github.com/bytebeamio/rumqtt
[crates.io]: https://crates.io/crates/uplink
[releases]: https://github.com/bytebeamio/uplink/releases
[action]: #
[action_response]: #
[docs.rs]: https://docs.rs/uplink
[coc]: docs/CoC.md
[contribute]: CONTRIBUTING.md

NOTE: Add link to doc/wiki about Actions and ActionResponses
