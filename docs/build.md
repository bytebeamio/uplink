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

1. Install `cross`, a `Zero setup` cross compilation crate.
```sh
cargo install cross
```
2. Build binary for the target architecture, e.g: `armv7-unknown-linux-gnueabihf`.
```sh
cross build --release --target armv7-unknown-linux-gnueabihf
```
3. Retreive executable from `/target/armv7-unknown-linux-gnueabihf/release/uplink` and execute it on target device.

## For Android systems

For an android target, the following instructions should help you build and install a compatible binary.
1. Install cargo-ndk v2.11.0
```sh
cargo install cargo-ndk --version 2.11.0
```
2. Ensure `ANDROID_NDK_HOME` env variable is set to point to the android NDK directory.
```sh
export ANDROID_NDK_HOME=~/Android/Sdk/ndk/25.0.8775105
```
3. Run the cargo-ndk tool to build uplink for the specific version of android
```sh
cargo ndk --target x86_64-linux-android --platform 23 build --bin uplink
```
4. Move executables to `/data/local/` on target device
5. Set the application's uplink port appropriately in the config.toml file
6. Include the following line in config.toml
```
persistence_path = "/data/local/uplink"
```
7. Ensure the uplink binary is executable and start it in the background
```sh
chmod +x uplink_exe
```

See [releases] for pre-compiled binaries to select platforms/architectures.

[releases]: https://github.com/bytebeamio/uplink/releases/