#!/bin/bash

set -xe

docker stop simulator noxious
docker network rm qa-jail
