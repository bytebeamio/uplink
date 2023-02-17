start_devices() {
    kill_devices;

    for id in $(seq 1 $1)
    do 
        mkdir -p devices
        # prepare config, please comment below line and download the right config from platform
        echo "{\"project_id\": \"demo\",\"device_id\": \"$id\",\"broker\": \"stage.bytebeam.io\",\"port\": 1883}" > devices/device_$id.json
        echo "processes = [] \naction_redirections = { send_file = \"load_file\" } \n\n[tcpapps.1] \nport = 500$id \nactions= [\"load_file\"] \n\n[downloader] \nactions= [\"send_file\"] \npath = \"/var/tmp/ota/$id\"" > devices/device_$id.toml
        start_uplink $id

        sleep 1
        start_simulaotr $id
    done
}

start_uplink() {
    nohup cargo run --bin uplink -- -a devices/device_$1.json -c devices/device_$id.toml -vv > devices/uplink_$1.log 2>&1 &
    echo $! >> devices/pids
}

start_simulaotr() {
    nohup cargo run --bin simulator -- localhost:500$1 ./paths > devices/simulator_$1.log 2>&1 &
    echo $! >> devices/pids
}

kill_devices() {
    while read pid
    do
      kill $pid
    done < devices/pids

    rm devices/pids
}

$1 $2