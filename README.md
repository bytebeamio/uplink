Setup CAN interface on ubuntu
--------------

https://stackoverflow.com/questions/33574256/socket-can-virtual-bus

```
ip link add dev vcan0 type vcan
ip link set up vcan0
```

Build for EC25
--------------

* Install rust
* Init quectel sdk to bring tools and env required for cross compilation into current shell
* ./ec25compile.sh
* Copy uplink binary and certs dir into ec25
* Run the below instruction in ec25

```
RUST_LOG=rumqtt=debug {path}/uplink --certs certs --bike bike-2 --rate 1
```

References
----------

* https://users.rust-lang.org/t/how-to-pass-cargo-linker-args/3163/2 
* https://sigmaris.info/blog/2019/02/cross-compiling-rust-on-mac-os-for-an-arm-linux-router/
