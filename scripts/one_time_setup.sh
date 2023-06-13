# Getting one_time_setup.sh
# curl  --proto '=https' --tlsv1.2 -sSf one_time_setup.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/examples/rpi/one_time_setup.sh | bash 

# get update_fstab.sh
curl --proto '=https' --tlsv1.2 -sSf -o update_fstab.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/update_fstab.sh
chmod +x ./update_fstab.sh
./update_fstab.sh
mount -a
cp update_fstab.sh /mnt/download/

# get update_fstab_next_root
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/update_fstab_next_root.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/update_fstab_next_root.sh

# get uplink binary
curl --proto '=https' --tlsv1.2 -sSfL -o /usr/local/share/bytebeam/uplink https://github.com/bytebeamio/uplink/releases/download/v2.4.0/uplink-aarch64-unknown-linux-gnu

# get systemd script
mkdir -pv /mnt/download/systemd
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/systemd.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/systemd/systemd.sh

# get uplink.service
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/uplink.service https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/systemd/uplink.service

# get startup.service
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/check-root-partition.service https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/systemd/startup.service

# get config.toml 
curl --proto '=https' --tlsv1.2 -sSf -o /usr/local/share/bytebeam/config.toml https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/config.toml

# get reboot.sh
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/reboot.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/reboot.sh

# get run_uplink.sh script
# curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/run_uplink.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/run_uplink.sh

# get startup.sh
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/check_root_part.sh https://raw.githubusercontent.com/bytebeamio/uplink/main/scripts/startup.sh

# Install netcat and vim
sudo apt install vim -y
sudo apt install netcat -y

# Make uplink executable
chmod +x /usr/local/share/bytebeam/uplink
chmod +x /mnt/download/check_root_part.sh

cp /mnt/download/systemd/uplink.service /etc/systemd/system/
cp /mnt/download/systemd/check-root-partition.service /etc/systemd/system/
systemctl daemon-reload
systemctl start check-root-part.service

touch /boot/two
touch /mnt/download/two
echo "Done!!! Place device.json in /mnt/download folder"
