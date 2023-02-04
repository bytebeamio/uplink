#! /bin/bash

set -e

function proxies() {
  http http://localhost:8474/proxies
}

function clean() {
  http DELETE http://localhost:8474/proxies/toxic-broker
}

function slowdown() {
  data='{
    "name": "slowdown",
    "type": "bandwidth",
    "toxicity": 1,
    "stream": "downstream",
    "attributes": {
        "rate": 1
    }
  }'

  echo $data | http http://localhost:8474/proxies/toxic-broker/toxics
}

function slowup() {
  data='{
    "name": "slowup",
    "type": "bandwidth",
    "toxicity": 1,
    "stream": "upstream",  
    "attributes": {
        "rate": 0
    }
  }'

  echo $data | http http://localhost:8474/proxies/toxic-broker/toxics
}

# Example
# proxy 4444 cloud.bytebeam.io:1883
# https://github.com/Shopify/toxiproxy#http-api
function proxy() {
  data='{
    "name":"toxic-broker",
    "enabled":true,
    "listen":"0.0.0.0:1883",
    "upstream":"stage.bytebeam.io:1883"
  }'

  echo $data | http http://localhost:8474/proxies
}

"$@"
