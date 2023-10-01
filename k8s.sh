#! /bin/bash

function deploy() {
  rustup target add aarch64-unknown-linux-musl
  export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc export CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc

  cargo build --target aarch64-unknown-linux-musl --release
  cp /tmp/cargo/aarch64-unknown-linux-musl/release/uplink uplink-agent

  docker build . -t localhost:32000/uplink:latest
  docker push localhost:32000/uplink:latest

  kubectl apply -f daemonset.yaml
}

function ping() {
  kubectl create deployment net-tools --image=wbitt/network-multitool -- ping 1.1.1.1
}

# https://github.com/projectcalico/calico/issues/5712
# calico has an issue
function reset() {
    kubectl delete pods --all --all-namespaces
}


case "$1" in
  deploy)
    deploy
    ;;
  reset)
    reset
    ;;
  *)
    echo "Usage: $0 {deploy|reset}"
    exit 1
esac


