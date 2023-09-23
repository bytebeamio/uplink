#!/bin/bash

set -xe

source qa-scripts/.env

docker build -t bytebeamio/simulator .
docker network create qa-jail
docker run --name noxious \
                 --rm -d \
                 -p 8474:8474 \
                 --network=qa-jail \
                 oguzbilgener/noxious
NOXIOUS_IP=$(docker inspect noxious | jq '.[].NetworkSettings.Networks."qa-jail".IPAddress' | tr -d '"')

toxiproxy-cli new mqtts --listen 0.0.0.0:8883 --upstream $CONSOLED_DOMAIN:8883
toxiproxy-cli new https --listen 0.0.0.0:443 --upstream firmware.$CONSOLED_DOMAIN:443

docker run --name simulator \
                --rm -d \
                --network=qa-jail \
                --add-host "$CONSOLED_DOMAIN:$NOXIOUS_IP" \
                --add-host "firmware.$CONSOLED_DOMAIN:$NOXIOUS_IP" \
                -e CONSOLED_DOMAIN=$CONSOLED_DOMAIN \
                -e BYTEBEAM_API_KEY=$BYTEBEAM_API_KEY \
                -it bytebeamio/simulator

docker exec -it simulator sv stop /etc/runit/uplink
docker exec -it simulator /usr/share/bytebeam/uplink/simulator.sh download_auth_config $DEVICE_ID
