#!/bin/bash

set -xe

source qa-scripts/.env

printf "$(cat << EOF
[console]
enabled = true
port = 3333
EOF
)" > devices/basics.toml
docker cp devices/basics.toml simulator:/usr/share/bytebeam/uplink/devices/basics.toml

docker exec -it simulator uplink -a /usr/share/bytebeam/uplink/devices/device_$DEVICE_ID.json -c /usr/share/bytebeam/uplink/devices/basics.toml -vv

# from separate terminal run the following to trigger minimum log level change(show debug logs of rumqttc)
# docker exec -it simulator curl -X POST -H "Content-Type: text/plain" -d "uplink=info,rumqtt=debug" http://localhost:3333/logs
