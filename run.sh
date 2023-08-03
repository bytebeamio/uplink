#!/bin/bash

echo "Hello, World"

TIME=
echo "{ \"action_id\": \"\", \"sequence\": \"0\", \"timestamp\": \"$(date +%s%N | cut -b1-13)\", \"state\": \"Completed\", \"progress\": 100, \"errors\": [] }"