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
    RUST_LOG=rumq_client=debug,uplink=warn target/debug/uplink -c config/uplink.toml -i $i -a certs/  2>&1 | tee /tmp/uplink-$i.txt &
done

trap 'kill $(jobs -p)' EXIT
wait
