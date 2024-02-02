#!/bin/bash

set -xe

source qa-scripts/.env

# printf "$(cat << EOF
# [downloader]
# actions = [{ name = "send_file", timeout = 1800 }]
# path = "/var/tmp/downloads"
# EOF
# )" > devices/downloader.toml
# docker cp devices/downloader.toml simulator:/usr/share/bytebeam/uplink/devices/downloader.toml

# docker exec -it simulator uplink -a /usr/share/bytebeam/uplink/devices/device_$DEVICE_ID.json -c /usr/share/bytebeam/uplink/devices/downloader.toml -vv -m uplink::base::bridge -m uplink::collector::downloader

# Trigger send_file action by first uploading file(choose a large file ~1GB):
# FILE_UUID=$(curl --location "https://$CONSOLED_DOMAIN/api/v1/file" --header "x-bytebeam-tenant: demo" --header "x-bytebeam-api-key: $BYTEBEAM_KEY" --form 'file=@"/path/to/file.txt"' --form 'fileName="file.txt"' | jq .id | tr -d '"')
# Push send_file to the designated device:
# FILE_SIZE=$(wc -c /path/to/file.txt | cut -d ' ' -f1) curl --location "https://$CONSOLED_DOMAIN/api/v1/actions" --header "x-bytebeam-tenant: demo" --header "x-bytebeam-api-key: $BYTEBEAM_KEY" --header 'Content-Type: application/json' --data "{ \"device_ids\": [\"$DEVICE_ID\"], \"search_type\": \"default\", \"search_key\": \"\", \"search_query\": \"string\", \"action\": \"send_file\", \"params\": { \"id\": \"$FILE_UUID\", \"url\":\"https://firmware.$CONSOLED_DOMAIN/api/v1/file/$FILE_UUID/artifact\", \"file_name\":\"file.txt\", \"content-length\":$FILE_SIZE }}"
# Slow down/stop the https network for downloader
# toxiproxy-cli toxic add -n slow -t latency -a latency=100 --downstream https
# toxiproxy-cli delete https
# Re-establish the https network
# toxiproxy-cli new https --listen 0.0.0.0:443 --upstream firmware.stage.bytebeam.io:443
# See logs of the download erroring out in between, restarting from where it had failed

# Restart uplink with partition attached that is smaller than the file to be downloaded
docker stop simulator
NOXIOUS_IP=$(docker inspect noxious | jq '.[].NetworkSettings.Networks."qa-jail".IPAddress' | tr -d '"')
docker run --name simulator \
                --rm -d \
                --network=qa-jail \
                --add-host "$CONSOLED_DOMAIN:$NOXIOUS_IP" \
                --add-host "firmware.$CONSOLED_DOMAIN:$NOXIOUS_IP" \
                -v /mnt/downloads:/var/tmp/downloads \
                -e CONSOLED_DOMAIN=$CONSOLED_DOMAIN \
                -e BYTEBEAM_API_KEY=$BYTEBEAM_API_KEY \
                -it bytebeamio/simulator

docker exec -it simulator sv stop /etc/runit/uplink
docker exec -it simulator /usr/share/bytebeam/uplink/simulator.sh download_auth_config $DEVICE_ID

printf "$(cat << EOF
[downloader]
actions = [{ name = "update_firmware" }, { name = "send_file" }, { name = "send_script" }]
EOF
)" > devices/downloader.toml
docker cp devices/downloader.toml simulator:/usr/share/bytebeam/uplink/devices/downloader.toml

docker exec -it simulator uplink -a /usr/share/bytebeam/uplink/devices/device_$DEVICE_ID.json -c /usr/share/bytebeam/uplink/devices/downloader.toml -vv -m uplink::base::bridge -m uplink::collector::downloader
# Trigger a large enough download to trigger the disk check to fail