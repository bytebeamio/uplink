start_devices() {
    kill_devices;

    for id in $(seq 1 $1)
    do 
        # prepare config, please comment below line and download the right config from platform
        # echo "{\"project_id\": \"demo\",\"device_id\": \"$id\",\"broker\": \"stage.bytebeam.io\",\"port\": 1883}" > device_$id.json
        echo "processes = [] \naction_redirections = { send_file = \"load_file\" } \n\n[tcpapps.1] \nport = 500$id \nactions= [\"load_file\"] \n\n[downloader] \nactions= [\"send_file\"] \npath = \"/var/tmp/ota/$id\"" > device_$id.toml
        start_uplink $id

        sleep 1
        start_app $id
    done
}

start_uplink() {
    nohup cargo run -- -a device_$1.json -c device_$id.toml -vv > uplink_$1.log 2>&1 &
    echo $! >> pids
}

start_app() {
    nohup python3 app.py $1 > app_$1.log 2>&1 &
    echo $! >> pids
}

kill_devices() {
    while read pid
    do
      kill $pid
    done < pids

    rm pids
}

$1 $2