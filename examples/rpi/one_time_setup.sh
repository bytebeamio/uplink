# Getting one_time_setup.sh
#curl -o one_time_setup.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/one_time_setup.sh -s

# Run uplink
#https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/demo.py
# get update_fstab.sh
curl -o update_fstab.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/update_fstab.sh -s
chmod +x update_fstab.sh
./update_fstab.sh
mount -a

# wget update_fstab_next_root_url -O /mnt/download/update_fstab.sh
# get update_fstab_next_root
curl -o /mnt/download/update_fstab_nextroot.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/update_fstab_next_root.sh -s 

# get uplink binary
curl -o /mnt/download/uplink -s -L https://github.com/bytebeamio/uplink/releases/download/v1.6.1/uplink-aarch64-unknown-linux-gnu

# get bridge_app
curl -o /mnt/download/bridge.py -s https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/bridge.py

# get systemd script
mkdir -pv /mnt/download/systemd
curl -o /mnt/download/systemd/systemd.sh -s https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/systemd/systemd.sh

# get uplink.service
curl -o /mnt/download/systemd/uplink.service -s https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/systemd/uplink.service

# get bridge.service
curl -o /mnt/download/systemd/bridge.service -s https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/systemd/bridge.service

# get config.toml 
curl -o /mnt/download/config.toml -s https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/config.toml

# Make uplink executable
chmod +x /uplink

ln -s /mnt/download/systemd/uplink.service /etc/systemd/system/multi-user.target.wants/uplink.service
ln -s /mnt/download/systemd/bridge.service /etc/systemd/system/multi-user.target.wants/bridge.service

systemctl daemon-reload

# get run_uplink.sh script
curl -o /mnt/download/run_uplink.sh -s https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/run_uplink.sh

chmod +x /mnt/download/run_uplink.sh

echo "Done!!! Place device.json in /mnt/download folder and run run_uplink.sh"
# Start uplink and bridge services
#systemctl start uplink.service
#systemctl start bridge.service
