# uplink

[![Rust][workflow-badge]][workflow] [![@bytebeamio][twitter-badge]][twitter]

<img align="right" src="docs/logo.png" height="150px" alt="the uplink logo">

Uplink is a rust based utility for efficiently sending data and receiving commands from and IoT Backend. The primary backend for uplink is the [**Bytebeam**][bytebeam] platform, however uplink can also be used with any broker supporting MQTT 3.1.

### Features
- JSON formatted data push to cloud.
- Receives commands from the cloud, executes them and updates progress of execution.
- Auto downloads updates from the cloud to perform OTA.
- Handles network interruptions by persisting data to disk.
- Remote shell access with [Tunshell][tunshell]
- Robust handling of flaky network conditions.
- Persist data to disk in case of network issues.
- Supports TLS.

### Build and Install

Build and install with [Cargo][crates.io]:

```sh
cargo install uplink # NOTE: This should work once crate is published
```

#### Build and run from source:

```sh
git clone https://github.com/bytebeamio/uplink.git
cd uplink
cargo run --bin uplink -- -a <device auth json file>
```
#### Build from source for ARM systems

In case you want to run uplink on an ARM system, follow instruction given below to create an ARM compatible binary.

1. Install `cross`, a `Zero setup` cross compilation crate.

```sh
cargo install cross
```
2. Build binary for the target `armv7-unknown-linux-gnueabihf`.
```sh
cross build --release --target armv7-unknown-linux-gnueabihf
```
3. Retreive executable from `/target/armv7-unknown-linux-gnueabihf/release/uplink` and execute it on target device.

See [releases][releases] for other options.

### Getting Started

You can use uplink with the following command, where you will need to provide an `auth.json` file, which you can read more about in the [uplink Security document][security]. On Bytebeam, you could download this file [from the platform][platform]. If you are using your own broker instead of Bytebeam, you could do it [without TLS][unsecure], but if you wish to use TLS we recommend that you also [provision your own certificates][provision].
```sh
uplink -a auth.json -vv
```

uplink acts as an intermediary between the user's applications and the Bytebeam platform. Connecting the applications to the bridge port one can communciate with the platform over MQTT with TLS, accepting JSON structured [Action][action]s and forwarding either JSON formatted data(from applications such as sensing) or [Action Response][action_response]s that report the progress of aforementioned Actions.

<img src="docs/uplink.png" height="150px" alt="uplink architecture">

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
[tunshell]: https://tunshell.com
[rumqtt]: https://github.com/bytebeamio/rumqtt
[crates.io]: https://crates.io/crates/uplink
[releases]: https://github.com/bytebeamio/uplink/releases
[action]: docs/actions.md/#Action
[action_response]: docs/actions.md/#ActionResponse
[security]: docs/security.md
[platform]: docs/security.md#Configuring-uplink-for-use-with-TLS
[unsecure]: docs/security.md#Using-uplink-without-TLS
[provision]: docs/security.md#Provisioning-your-own-certificates
[docs.rs]: https://docs.rs/uplink
[coc]: docs/CoC.md
[contribute]: CONTRIBUTING.md
