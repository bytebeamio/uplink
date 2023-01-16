# Getting one_time_setup.sh
#curl -s -o one_time_setup.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/one_time_setup.sh

# Run uplink

# get update_fstab.sh
curl -s -o update_fstab.sh https://raw.githubusercontent.com/sai-kiran-y/uplink/rpi/examples/rpi/update_fstab.sh
chmod +x update_fstab.sh
./update_fstab.sh
mount -a

# wget update_fstab_next_root_url -O /mnt/download/update_fstab.sh
# get update_fstab_next_root
curl -s -o /mnt/download/update_fstab_nextroot.sh https://github.com/sai-kiran-y/uplink/blob/rpi/examples/rpi/update_fstab_next_root.sh 

#wget uplink_url -O /mnt/download/uplink
#wget bridge_app_url -O /mnt/download/bridge.py
#wget systemd_url -O /mnt/download/systemd/systemd.sh
#wget uplink.service_url -O /mnt/download/systemd/uplink.service
#wget config.toml -O /mnt/download/config.toml 
#chmod +x /uplink

# uplink.service executes startup.sh, which runs both uplink and bridge app
#ln -s /mnt/download/systemd/uplink.service /etc/systemd/system/multi-user.target.wants/uplink.service
#systemctl daemon-reload
#systemctl start uplink.service
