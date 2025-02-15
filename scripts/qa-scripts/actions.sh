#!/bin/bash

set -xe

source qa-scripts/.env

printf "$(cat << EOF
persistence_path = "/var/tmp/persistence"
action_redirections = { "send_file" = "load_file", "send_script" = "run_script" }
script_runner = [{ name = "run_script" }]

[downloader]
actions = [{ name = "send_file" },{ name = "send_script"}]
path = "/var/tmp/downloads"

[simulator]
actions = ["load_file"]
gps_paths = "./paths/"

[tcpapps.blackhole]
port = 7891
actions = [{ name = "no_response", timeout = 100 }, { name = "restart_response", timeout = 1800 }]
EOF
)" > devices/actions.toml
docker cp devices/actions.toml simulator:/usr/share/bytebeam/uplink/devices/actions.toml

docker exec -it simulator uplink -a /usr/share/bytebeam/uplink/devices/device_$DEVICE_ID.json -c /usr/share/bytebeam/uplink/devices/actions.toml -vv -m uplink::base::bridge

# Trigger send_file action by first uploading the file(similar for send_file):
# FILE_UUID=$(curl --location "https://$CONSOLED_DOMAIN/api/v1/file" --header "x-bytebeam-tenant: demo" --header "x-bytebeam-api-key: $BYTEBEAM_KEY" --form 'file=@"/path/to/file.txt"' --form 'fileName="file.txt"' | jq .id | tr -d '"')
# Push send_file action to the designated device:
# FILE_SIZE=$(wc -c /path/to/file.txt | cut -d ' ' -f1) curl --location "https://$CONSOLED_DOMAIN/api/v1/actions" --header "x-bytebeam-tenant: demo" --header "x-bytebeam-api-key: $BYTEBEAM_KEY" --header 'Content-Type: application/json' --data "{ \"device_ids\": [\"$DEVICE_ID\"], \"search_type\": \"default\", \"search_key\": \"\", \"search_query\": \"string\", \"action\": \"send_file\", \"params\": { \"id\": \"$FILE_UUID\", \"url\":\"https://firmware.$CONSOLED_DOMAIN/api/v1/file/$FILE_UUID/artifact\", \"file_name\":\"file.txt\", \"content-length\":$FILE_SIZE }}"
# Trigger another action when first action is in execution on the device to see actions being rejected
# Stop uplink when an action is still in execution and check persistence directory for current_action file
# docker exec -it simulator tree /var/tmp/persistence
# Restart uplink and connect with example python to send respose to persisted action(restart_response action)
# Do the same with another action(no_response action) see if on timeout a failure response is created when the action handler doesn't send any response