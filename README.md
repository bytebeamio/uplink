# uplink

[![Rust](https://github.com/bytebeamio/uplink/actions/workflows/rust.yml/badge.svg)](https://github.com/bytebeamio/uplink/actions/workflows/rust.yml) [![@bytebeamio](https://twitter.com/intent/follow?screen_name=bytebeamio)](https://img.shields.io/twitter/follow/bytebeamio.svg?style=social&label=Follow)

<img align="right" src="docs/logo.png" height="150px" alt="the uplink logo">

uplink is a *utility* and a *library* written in Rust to connect multiple devices running locally with the [**Bytebeam**](https://bytebeam.io) platform.

### Features

- Customizable, with easy to write configuration files in TOML/JSON that help you use uplink to meet your needs.
- Supports TLS by default, with [rumqtt](https://github.com/bytebeamio/rumqtt).

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

See [releases](https://github.com/bytebeamio/uplink/releases) for other options.

### Getting Started

Try running an instance with certificates.json sourced from the platform:

```sh
uplink -a certificates.json -vv # use `cargo run --bin uplink --` incase uplink isn't installed
```

### Testing with netcat

- Send content of a text file

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

### Build for Beagle

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


You can find a deeper introduction, examples, and environment setup guides in
the [manual](https://deno.land/manual).

The complete API reference is available in our
[documentation][docs.rs].

### Contributing

We appreciate your help!

To contribute, please read our
[contributor guide](CONTRIBUTE.md).


Uplink is a tool and a library that you can use to receive and send data from the [Bytebeam](https://bytebeam.io) platform.

[crates.io]: https://crates.io/crates/uplink
[docs.rs]: (https://docs.rs/uplink)
