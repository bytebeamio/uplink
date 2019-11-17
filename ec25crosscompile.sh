#! /bin/bash


set -ex

mkdir .cargo | true

echo "#!/bin/sh" > .cargo/gcc.sh
echo "$CC --sysroot=$QL_SDK_TARGET_SYSROOT \$@" >> .cargo/gcc.sh

chmod +x .cargo/gcc.sh

echo '[target.arm-unknown-linux-gnueabi]' > .cargo/config
echo 'linker = '\""$PWD/.cargo/gcc.sh"\">> .cargo/config
echo
echo "[build]" >> .cargo/config
echo "rustflags = [\"-C\", \"rpath\"]" >> .cargo/config

rustup target add arm-unknown-linux-gnueabi
cargo build --release --target=arm-unknown-linux-gnueabi
