# Adds uplink and bridge to systemd

# To be executed after the rootfs is extracted to /mnt/next_root
cp /mnt/download/systemd/uplink.service /mnt/next_root/etc/systemd/system/
cp /mnt/download/systemd/bridge.service /mnt/next_root/etc/systemd/system/
