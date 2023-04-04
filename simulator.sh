#!/bin/bash

start_devices() {
    start=${1:?"Missing start id"}
    stop=${2:?"Missing end id"}
    kill_devices;
    mkdir -p devices

    echo "Starting uplink and simulator"
    for id in $(seq $start $stop)
    do 
        download_auth_config $id
        create_uplink_config $id
        start_uplink $id

        sleep 1
        start_simulaotr $id
    done
    echo DONE

    # Wait on pids and block till atleast one uplink is running
    while read pid
    do
      tail --pid=$pid -f /dev/null
    done < devices/pids
}

create_uplink_config() {
    printf "processes = [] \naction_redirections = { send_file = \"load_file\", update_firmware = \"install_firmware\" } \n\n[tcpapps.1] \nport = 500$1 \nactions= [{ name = \"load_file\" }, { name = \"install_firmware\" }, { name = \"update_config\" }, { name = \"unlock\" }, { name = \"lock\" }] \n\n[downloader] \nactions= [{ name = \"send_file\" }, { name = \"update_firmware\" }] \npath = \"/var/tmp/ota/$1\"" > devices/device_$1.toml
}

download_auth_config() {
    id=${1:?"Missing id"}
    url="https://$CONSOLED_DOMAIN/api/v1/devices/$id/cert"
    echo "Downloading config: $url"
    mkdir -p devices
    curl --location $url \
        --header 'x-bytebeam-tenant: demo' \
        --header "x-bytebeam-api-key: $BYTEBEAM_API_KEY" > devices/device_$id.json
}

start_uplink() {
    nohup uplink -a devices/device_$1.json -c devices/device_$1.toml -vv > devices/uplink_$1.log 2>&1 &
    echo $! >> devices/pids
}

start_simulaotr() {
    nohup simulator -p 500$1 -g ./paths -vvv > devices/simulator_$1.log 2>&1 &
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

${1:?"Missing command"} ${@:2}
