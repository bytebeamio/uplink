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

#### Setup
You can start uplink with the following command, where you will need to provide an `auth.json` file:
```sh
uplink -a auth.json -vv
```

The `auth.json` file might contain something similar to the following JSON, pointing uplink to a certain broker and providing necessary information to initiate an authenticated connection over TLS:
```js
{
    "project_id": "xxxx",
    "device_id": "1234",
    "broker": "example.com",
    "port": 8883,
    "authentication": {
        "ca_certificate": "...",
        "device_certificate": "...",
        "device_private_key": "..."
    }
}
```

> **NOTE**: You could download this file [from the Bytebeam UI][platform]. If you are using your own broker instead, you could use uplink [without TLS][unsecure], but we recommend that you use TLS and [provision your own certificates][provision] to do it. You can read more about securing uplink in the [uplink Security document][security]

#### Writing Applications
uplink acts as an intermediary between the user's applications and the Bytebeam platform. Connecting the applications to uplink's bridge port one can communciate with the platform over MQTT + TLS, accepting JSON structured [Action][action]s and forwarding either JSON formatted data(from applications such as sensing) or [Action Response][action_response]s that report the progress of aforementioned Actions.

**Receiving Actions**:
Actions are messages that uplink receives from the broker and is executable on the user's device. It can execute some specific types of actions while forwarding a vast majority for execution by connected applicaitons. Developers can write their application to receive a JSON formatted as follows:
```js
{
    "action_id": "...",
    "kind": "process",
    "name": "update_firmware",
    "payload": {...}
}
```

**Streaming data**:
Data from the connected application is handled as payload in a stream. An example Streamed Payload is JSON formatted as follows:
```js
{
    "stream": "action_status",
    "sequence": ...,
    "timestamp": ...,
    "payload": {...},
}
```
> **NOTE**: data contained in payload must be JSON formatted as it will be deserialized for a variety of purposes upstream.

**Responding with Action Responses**:
Action Responses are messages that applications can use to update uplink regarding the execution of any received action, which it then forwards to the broker. These usually contain information such as progress counter and a backtrace in cause of failure due to error. Action Responses are handled as Streamed Payload in the "action_status" stream and thus have to be enclosed as such. Developers can write their application to respond with a JSON formatted as follows:
```js
{
    "action_id": "...",
    "timestamp": ...,
    "state": "Running",
    "progress": 0,
    "errors": []
}
```

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
[action]: docs/apps.md/#Action
[action_response]: docs/apps.md/#ActionResponse
[security]: docs/security.md
[platform]: docs/security.md#Configuring-uplink-for-use-with-TLS
[unsecure]: docs/security.md#Using-uplink-without-TLS
[provision]: docs/security.md#Provisioning-your-own-certificates
[docs.rs]: https://docs.rs/uplink
[coc]: docs/CoC.md
[contribute]: CONTRIBUTING.md
