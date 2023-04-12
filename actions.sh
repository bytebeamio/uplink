#!/bin/bash

consoled_url="https://$CONSOLED_DOMAIN/api/v1"
version=$1
start=$2
stop=$3
devices=$(seq -s ", " -f '"%g"' $start $stop)

echo "update_firmware action for v$version will be triggered on $devices"

curl --fail -X POST "$consoled_url/actions" \
-H "x-bytebeam-api-key: $BYTEBEAM_API_KEY" \
-H "x-bytebeam-tenant: $BYTEBEAM_TENANT" \
-H 'Content-Type: application/json' \
--data "{
  \"device_ids\": [$devices],
  \"search_type\": \"default\",
  \"search_key\": \"\",
  \"search_query\": \"string\",
  \"action\": \"update_firmware\",
  \"params\": {
    \"version\": \"$version\"
  }
}"