# Getting one_time_setup.sh
# curl  --proto '=https' --tlsv1.2 -sSf one_time_setup.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/one_time_setup.sh | bash 

# get update_fstab.sh
curl --proto '=https' --tlsv1.2 -sSf -o update_fstab.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/update_fstab.sh
chmod +x ./update_fstab.sh
./update_fstab.sh
mount -a
cp update_fstab.sh /mnt/download/

# get update_fstab_next_root
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/update_fstab_next_root.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/update_fstab_next_root.sh

# get uplink binary
curl --proto '=https' --tlsv1.2 -sSfL -o /mnt/download/uplink https://github.com/bytebeamio/uplink/releases/download/v1.6.1/uplink-aarch64-unknown-linux-gnu

# get bridge_app
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/bridge.py https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/bridge.py

# get systemd script
mkdir -pv /mnt/download/systemd
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/systemd.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/systemd/systemd.sh

# get uplink.service
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/uplink.service https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/systemd/uplink.service

# get bridge.service
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/bridge.service https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/systemd/bridge.service

# get config.toml 
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/config.toml https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/config.toml

# Make uplink executable
chmod +x /mnt/download/uplink

cp /mnt/download/systemd/uplink.service /etc/systemd/system/
cp /mnt/download/systemd/bridge.service /etc/systemd/system/
systemctl daemon-reload

# get run_uplink.sh script
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/run_uplink.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/run_uplink.sh

chmod +x /mnt/download/run_uplink.sh

echo "Done!!! Place device.json in /mnt/download folder and run the script run_uplink.sh"
