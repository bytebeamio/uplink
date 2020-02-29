Setup CAN interface on ubuntu
--------------

https://stackoverflow.com/questions/33574256/socket-can-virtual-bus

```
ip link add dev vcan0 type vcan
ip link set up vcan0
```

```
RUST_LOG=debug cargo run -- -a certs/ -c config/uplink.toml -i bike-1
```

Build for Beagle
--------------
Install arm compilers and linkers

```
apt install gcc-9-arm-linux-gnueabihf
ln -s /usr/bin/arm-linux-gnueabihf-gcc-9 /usr/bin/arm-linux-gnueabihf-gcc
```
create `.cargo/config`

```
[target.armv7-unknown-linux-gnueabihf]
linker = "arm-linux-gnueabihf-gcc"

[build]
rustflags = ["-C", "rpath"]
```

```
rustup target install armv7-unknown-linux-gnueabihf
cargo build --release --target armv7-unknown-linux-gnueabihf
```

References
----------
* [Rust target list to arm architecture map](https://forge.rust-lang.org/release/platform-support.html)
* [Arm architectures](https://en.wikipedia.org/wiki/List_of_ARM_microarchitectures)
* https://users.rust-lang.org/t/how-to-pass-cargo-linker-args/3163/2 
* https://sigmaris.info/blog/2019/02/cross-compiling-rust-on-mac-os-for-an-arm-linux-router/
