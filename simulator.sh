#!/bin/bash

start_devices() {
    kill_devices;


    echo "Starting uplink and simulator"
    for id in $(seq 1 $1)
    do 
        mkdir -p devices
        # prepare config, please comment below line and download the right config from platform
        # echo "{\"project_id\": \"demo\",\"device_id\": \"$id\",\"broker\": \"stage.bytebeam.io\",\"port\": 1883}" > devices/device_$id.json
        printf "processes = [] \naction_redirections = { send_file = \"load_file\", update_firmware = , \"install_firmware\" } \n\n[tcpapps.1] \nport = 500$id \nactions= [\"load_file\", \"install_firmware\"] \n\n[downloader] \nactions= [\"send_file\", \"update_firmware\"] \npath = \"/var/tmp/ota/$id\"" > devices/device_$id.toml
        start_uplink $id

        sleep 1
        start_simulaotr $id
    done

    echo DONE
}

start_uplink() {
    nohup ./target/release/uplink -a devices/device_$1.json -c devices/device_$1.toml -vv > devices/uplink_$1.log 2>&1 &
    echo $! >> devices/pids
}

start_simulaotr() {
    nohup ./target/release/simulator -p 500$1 -g ./paths -vvv > devices/simulator_$1.log 2>&1 &
    echo $! >> devices/pids
}

kill_devices() {
    echo "Killing all devices in pids file"
    i=1
    while read pid
    do
      echo -ne "$i"
      kill $pid
      i=`expr $i + 1`
    done < devices/pids

    rm devices/pids
    echo DONE
}

$1 $2