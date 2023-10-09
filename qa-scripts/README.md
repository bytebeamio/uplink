# QA

The scripts to automate setting up QA testing environ are as such:
- `setup.sh`: Create two container proxy system for simulating toxic internet with uplink on one side.
- `basic.sh`: Run uplink with built-in config to verify basic setup and operation.
- `streams.sh`: Runs uplink with certains streams configured to push data onto network faster than normal, observe logs to verify required behavior.
- `persistence.sh`: Runs uplink and provides guidance on how to trigger network toxics to trigger uplink into persistance mode, observe logs to verify required behavior.
- `actions.sh`: Runs uplink configured to accept actions, with guidance on how to trigger the action and observe logs to verify required behavior.
- `downloader.sh`: Runs uplink configured to accept download actions that can then be slowed down with toxics as guided to observe and verify required behavior.
- `cleanup.sh`: Pull down what was created in `setup.sh`.

## Miscellaneous
### Tunshell
Start uplink in the simulator container:
```sh
docker exec -it simulator uplink -a /usr/share/bytebeam/uplink/devices/device_$DEVICE_ID.json -vvv -m uplink::collector::tunshell -m uplink::base::bridge
```
Trigger multiple tunshell sessions from platform to ensure that connections are established and even with another session or action(like "send_file") in progress a new session is allowed to connect.

### Device Shadow
Start uplink in the simulator container:
```sh
printf "$(cat << EOF
[device_shadow]
interval = 10
EOF
)" > devices/shadow.toml
docker cp devices/shadow.toml simulator:/usr/share/bytebeam/uplink/devices/shadow.toml

docker exec -it simulator uplink -a /usr/share/bytebeam/uplink/devices/device_$DEVICE_ID.json -c /usr/share/bytebeam/uplink/devices/shadow.toml -vvv -m uplink::collector::device_shadow
```
Ensure the logs are timed 10s between
