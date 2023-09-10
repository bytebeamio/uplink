#!/bin/bash

start_devices() {
    start=${1:?"Missing start id"}
    stop=${2:?"Missing end id"}
    kill_devices;
    mkdir -p devices

    echo "Starting uplink with simulator"
    devices=($(seq $start $stop))
    first=${devices[0]}
    rest=${devices[@]:1}
    download_auth_config $first
    create_uplink_config $first
    start_uplink 1 $first "-vv" "devices/uplink_$first.log"

    for id in $rest
    do 
        sleep 1

        download_auth_config $id
        create_uplink_config $id
        start_uplink 0 $id
    done
    echo DONE

    # Wait on pids and block till atleast one uplink is running
    for file in $(find ./devices -type f -name "*.pid")
    do
      tail --pid=$(cat $file) -f /dev/null
    done
}

create_uplink_config() {
    id=${1:?"Missing id"}
    printf "$(cat << EOF
processes = [] 
action_redirections = { send_file = \"load_file\", update_firmware = \"install_firmware\" }
persistence_path = \"/var/tmp/persistence/$id\"

[persistence]
max_file_size = 104857600
max_file_count = 3

[streams.gps]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/gps/jsonarray"
persistence = { max_file_size = 0 }

[streams.bms]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/bms/jsonarray"
persistence = { max_file_size = 0 }

[streams.imu]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/imu/jsonarray"
persistence = { max_file_size = 0 }

[streams.motor]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/motor/jsonarray"
persistence = { max_file_size = 0 }

[streams.peripheral_state]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/peripheral_state/jsonarray"
persistence = { max_file_size = 0 }

[streams.device_shadow]
topic = "/tenants/{tenant_id}/devices/{device_id}/events/device_shadow/jsonarray"
persistence = { max_file_size = 0 }

[simulator]
gps_paths = "./paths"
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
    echo $! >> "devices/$2.pid"
}

kill_devices() {
    echo "Killing all devices in pids file"
    for file in $(find ./devices -type f -name "*.pid")
    do
      kill_device $file
    done

    echo DONE
}

kill_device() {
    echo "Killing $1"
    kill $(cat $1)
    rm $1
}

${1:?"Missing command"} ${@:2}
