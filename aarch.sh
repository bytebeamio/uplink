#! /bin/bash

rustup target add aarch64-unknown-linux-musl
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc export CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc
cargo build --target aarch64-unknown-linux-musl --release

cp /tmp/cargo/aarch64-unknown-linux-musl/release/uplink uplink-agent
