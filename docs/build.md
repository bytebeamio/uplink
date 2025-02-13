# Build uplink from source

1. Get source code from github:
```sh
git clone https://github.com/bytebeamio/uplink.git
```
2. Build uplink your system's architecture, the binary should be in `target/release/`
```sh
cargo build --release
```
## For ARM systems

In case you want to run uplink on an ARM system, follow instruction given below to create an ARM compatible binary.

* Install `cross`. This tool uses docker to make cross compilation easier.
```sh
cargo install cross@0.2.4
```
* Install docker on your computer. Follow the instructions for your operating system from https://docker.io
* Build binary for the target architecture, e.g: `armv7-unknown-linux-gnueabihf`.
```sh
cross build --release --target armv7-unknown-linux-gnueabihf
```
* Retreive executable from `/target/<target-architecture>>/release/uplink` and execute it on target device.

## For Android systems
See [uplink-android](https://github.com/bytebeamio/uplink-android)

See [releases] for pre-compiled binaries to select platforms/architectures.

[releases]: https://github.com/bytebeamio/uplink/releases/