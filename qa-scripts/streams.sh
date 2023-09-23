#!/bin/bash

set -xe

source qa-scripts/.env

printf "$(cat << EOF
[simulator]
actions = []
gps_paths = "./paths/"

[streams.bms]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/bms/jsonarray"
flush_period = 2

[streams.imu]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/imu/jsonarray"
buf_size = 10
EOF
)" > devices/streams.toml
docker cp devices/streams.toml simulator:/usr/share/bytebeam/uplink/devices/streams.toml

docker exec -it simulator uplink -a /usr/share/bytebeam/uplink/devices/device_$DEVICE_ID.json -c /usr/share/bytebeam/uplink/devices/streams.toml -vv -m uplink::base::bridge
