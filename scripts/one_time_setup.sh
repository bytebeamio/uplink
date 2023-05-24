# Getting one_time_setup.sh
# curl  --proto '=https' --tlsv1.2 -sSf one_time_setup.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/main/examples/rpi/one_time_setup.sh | bash 

# get update_fstab.sh
curl --proto '=https' --tlsv1.2 -sSf -o update_fstab.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/update_fstab.sh
chmod +x ./update_fstab.sh
./update_fstab.sh
mount -a
cp update_fstab.sh /mnt/download/

# get update_fstab_next_root
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/update_fstab_next_root.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/update_fstab_next_root.sh

# get uplink binary
curl --proto '=https' --tlsv1.2 -sSfL -o /etc/bytebeam/uplink https://github.com/bytebeamio/uplink/releases/download/v2.3.0-rc/uplink-aarch64-unknown-linux-gnu

# get systemd script
mkdir -pv /mnt/download/systemd
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/systemd.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/systemd/systemd.sh

# get uplink.service
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/uplink.service https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/systemd/uplink.service

# get startup.service
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/systemd/startup.service https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/systemd/startup.service

# get config.toml 
curl --proto '=https' --tlsv1.2 -sSf -o /etc/bytebeam/config.toml https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/config.toml

# get reboot.sh
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/reboot.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/reboot.sh

# get run_uplink.sh script
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/run_uplink.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/run_uplink.sh

# get startup.sh
curl --proto '=https' --tlsv1.2 -sSf -o /mnt/download/startup.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/startup.sh

# Install netcat and vim
sudo apt install vim -y
sudo apt install netcat -y

# Make uplink executable
chmod +x /etc/bytebeam/uplink
chmod +x /mnt/download/startup.sh

cp /mnt/download/systemd/uplink.service /etc/systemd/system/
cp /mnt/download/systemd/startup.service /etc/systemd/system/
systemctl daemon-reload

chmod +x /mnt/download/run_uplink.sh
echo "Done!!! Place device.json in /mnt/download folder and run the script run_uplink.sh"
