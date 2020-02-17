 #!/bin/bash

set -ex

if [[ $# -ne 1 ]]; then
    echo "Pass number of devices"
    echo "Usage: ./createvehicle 10"
    exit
fi

count=$1

for i in $(seq 1 $count)
do
    RUST_LOG=rumq_broker,uplink=debug cargo run -- -c config/uplink.toml -i $i -a certs/ &
done

trap 'kill $(jobs -p)' EXIT
wait
