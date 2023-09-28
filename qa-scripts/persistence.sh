#!/bin/bash

set -xe

source qa-scripts/.env

printf "$(cat << EOF
persistence_path = "/var/tmp/persistence"

[simulator]
actions = []
gps_paths = "./paths/"

[streams.motor]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/gps/jsonarray"

[streams.bms]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/bms/jsonarray"
persistence = { max_file_size = 0 }

[streams.imu]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/imu/jsonarray"
persistence = { max_file_count = 3, max_file_size = 1024 }
EOF
)" > devices/persistence.toml
docker cp devices/persistence.toml simulator:/usr/share/bytebeam/uplink/devices/persistence.toml

docker exec -it simulator uplink -a /usr/share/bytebeam/uplink/devices/device_$DEVICE_ID.json -c /usr/share/bytebeam/uplink/devices/persistence.toml -vv -m uplink::base::serializer -m storage

# Slow down mqtts
# toxiproxy-cli toxic add -n slow -t latency -a latency=100 --downstream mqtts
# Look at logs for persistence into disk in slow mode, catchup mode
# Disrupt mqtts, check logs for slow mode data loss on in-memory buffer overflow, etc.
# toxiproxy-cli delete mqtts
# Verify persistence of packets onto disk
# docker exec -it simulator tree /var/tmp/persistence
# Bring back network, check logs for back to normal mode, check platform for appropriate data retention/loss with timestamp gaps
# toxiproxy-cli new mqtts --listen 0.0.0.0:8883 --upstream $CONSOLED_DOMAIN:8883