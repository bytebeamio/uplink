#! /bin/bash

set -e

function proxies() {
	http http://localhost:8474/proxies
}

function clean() {
	http DELETE http://localhost:8474/proxies/toxic-broker
}

function reset {
	http POST http://localhost:8474/reset
}

# Example
# proxy 4444 cloud.bytebeam.io:1883
# https://github.com/Shopify/toxiproxy#http-api
function proxy() {
	data='{
    "name":"toxic-broker",
    "enabled":true,
    "listen":"0.0.0.0:8883",
    "upstream":"cloud.bytebeam.io:8883"
  }'

	echo $data | http http://localhost:8474/proxies
}

function slowdown() {
	data='{
    "name": "slowdown",
    "type": "bandwidth",
    "toxicity": 1,
    "stream": "downstream",
    "attributes": {
        "rate": 0
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

function 3g() {
	reset

	data='{
    "name": "3gup",
    "type": "bandwidth",
    "toxicity": 1,
    "stream": "upstream",  
    "attributes": {
        "rate": 1024
    }
  }'

	echo $data | http http://localhost:8474/proxies/toxic-broker/toxics

	data='{
    "name": "3gdown",
    "type": "bandwidth",
    "toxicity": 1,
    "stream": "upstream",  
    "attributes": {
        "rate": 1024
    }
  }'

	echo $data | http http://localhost:8474/proxies/toxic-broker/toxics
}

function 2g() {
	reset

	data='{
    "name": "2gup",
    "type": "bandwidth",
    "toxicity": 1,
    "stream": "upstream",  
    "attributes": {
        "rate": 20
    }
  }'

	echo $data | http http://localhost:8474/proxies/toxic-broker/toxics

	data='{
    "name": "2gdown",
    "type": "bandwidth",
    "toxicity": 1,
    "stream": "upstream",  
    "attributes": {
        "rate": 20
    }
  }'

	echo $data | http http://localhost:8474/proxies/toxic-broker/toxics
}

"$@"
