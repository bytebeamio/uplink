#!/bin/bash

start_devices() {
    start=${1:?"Missing start id"}
    stop=${2:?"Missing end id"}
    kill_devices;
    mkdir -p devices

    echo "Starting uplink and simulator"
    devices=($(seq $start $stop))
    first=${devices[0]}
    rest=${devices[@]:1}
    printf -v port "50%03d" $first
    download_auth_config $first
    create_uplink_config $first $port
    start_uplink 1 $first "-vv" "devices/uplink_$first.log"

    sleep 1
    start_simulator 1 $first $port "-vv" "devices/simulator_$first.log"

    for id in $rest
    do 
        printf -v port "50%03d" $id
        download_auth_config $id
        create_uplink_config $id $port
        start_uplink 0 $id

        sleep 1
        start_simulator 0 $id $port
    done
    echo DONE

    # Wait on pids and block till atleast one uplink is running
    while read pid
    do
      tail --pid=$pid -f /dev/null
    done < devices/pids
}

create_uplink_config() {
    id=${1:?"Missing id"}
    port=${2:?"Missing port number"}
    printf "$(cat << EOF
processes = [] 
action_redirections = { send_file = \"load_file\", update_firmware = \"install_firmware\" }

[persistence]
path = \"/var/tmp/persistence/$id\"
max_file_size = 104857600
max_file_count = 3

[tcpapps.1]
port = $port
actions= [{ name = \"load_file\" }, { name = \"install_firmware\" }, { name = \"update_config\" }, { name = \"unlock\" }, { name = \"lock\" }]

[downloader]
actions= [{ name = \"send_file\" }, { name = \"update_firmware\" }]
path = \"/var/tmp/ota/$id\"
EOF
)" > devices/device_$id.toml
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

run() {
    printf "running: $2 $3"
    if [ $1 -eq 1 ]
    then
        echo " > $4"
        nohup $2 $3 > $4 2>&1 &
    else
        $2 &
    fi
}

start_uplink() {
    echo "Starting uplink with device_id: $2"
    cmd="uplink -a devices/device_$2.json -c devices/device_$2.toml"
    run $1 "$cmd" "$3" "$4"
    echo $! >> devices/pids
}

start_simulator() {
    nohup=$1
    id=${2:?"Missing id"}
    port=${3:?"Missing port number"}
    cmd="simulator -p $port -g ./paths"
    run $1 "$cmd" "$4" "$5"
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
