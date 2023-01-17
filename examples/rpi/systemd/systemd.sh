# Adds uplink and bridge to systemd

# To be executed after the rootfs is extracted to /mnt/next_root
ln -s /mnt/download/systemd/uplink.service /mnt/next_root/etc/systemd/system/multi-user.target.wants/uplink.service
ln -s /mnt/download/systemd/bridge.service /mnt/next_root/etc/systemd/system/multi-user.target.wants/bridge.service
