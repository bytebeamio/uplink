# uplink

[![Rust](https://github.com/bytebeamio/uplink/actions/workflows/rust.yml/badge.svg)](https://github.com/bytebeamio/uplink/actions/workflows/rust.yml) [![@bytebeamio](https://img.shields.io/twitter/follow/bytebeamio.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=bytebeamio)

<img align="right" src="docs/logo.png" height="150px" alt="the uplink logo">

uplink is a *utility/library* written in Rust for connecting devices running a supported OS with an MQTT backend. The primary target for uplink is the [**Bytebeam**](https://bytebeam.io) platform, however uplink can also be used with any broker supporting MQTT 3.1.

### Features

- Customizable, with easy to write configuration files in TOML/JSON that help you use uplink to meet your needs.
- Supports TLS by default, with [rumqtt](https://github.com/bytebeamio/rumqtt).
- JSON formatted data push to cloud.
- Receives commands from the cloud, executes them and updates progress of execution.
- Auto downloads updates from the cloud to perform OTA.

### Install

Build and install with [Cargo][crates.io]:

```sh
cargo install uplink # NOTE: This should work in the future
```

Build and run from source from:

```sh
git clone https://github.com/bytebeamio/uplink.git
cd uplink
cargo run --bin uplink -- -a <device auth json file>
```

See [releases][releases] for other options.

### Getting Started

Try running an instance with certificates.json sourced from the platform:

```sh
uplink -a certificates.json -vv # use `cargo run --bin uplink --` incase uplink isn't installed
```
### Architecture
uplink acts as an intermediary between a user's applications and the Bytebeam platform/broker. Connecting the applications to the bridge port one can communciate with the platform over MQTT with TLS, accepting JSON structured [Action][action]s and forwarding either JSON formatted data(from applications such as sensing) or [Action Response][action_response]s that report the progress of aforementioned Actions.

<img src="docs/uplink.png" height="150px" alt="uplink architecture">

Implementation details are included in the document

### Secure Communication
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

- Send content of a text file to the bridge port

```sh
nc localhost 5555 < tools/netcat.txt
```

- Send json one by one

```sh
nc localhost 5555
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
```

### Build for ARM

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

### References

* [Rust target list to arm architecture map](https://forge.rust-lang.org/release/platform-support.html)
* [Arm architectures](https://en.wikipedia.org/wiki/List_of_ARM_microarchitectures)
* https://users.rust-lang.org/t/how-to-pass-cargo-linker-args/3163/2 
* https://sigmaris.info/blog/2019/02/cross-compiling-rust-on-mac-os-for-an-arm-linux-router/

The complete API reference for the uplink library is available within the [library documentation][docs.rs].

### Contributing
Please follow the [code of conduct][coc] while opening issues to report bugs or before you contribute fixes, also do read our [contributor guide](CONTRIBUTE.md) to get a better idea of what we'd appreciate and what we won't.

[crates.io]: https://crates.io/crates/uplink
[releases]: https://github.com/bytebeamio/uplink/releases
[action]: #
[action_response]: #
[docs.rs]: https://docs.rs/uplink
[coc]: #

NOTE: Add link to doc/wiki about Actions and ActionResponses
