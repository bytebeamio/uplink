#!/bin/bash
## Step 1: Extracting the rootfs
rm -rf /mnt/next_root/*
tar -xvpzf backup.tar.gz -C /mnt/next_root/

## Update the fstab of extracted rootfs

# Get the root partition info
root_part=`awk -F"root=" '{ print $NF; }' /proc/cmdline | cut -d" " -f1`

# Set download partition to /dev/mmcblk0p4
download_part=`echo $root_part | sed 's/.$/4/'`

# If root_part is 2, set next_root to 3 and vice versa
if [ ${root_part: -1} = "2" ]
then
	echo "part_two"
	next_root=`echo $root_part | sed  's/.$/3/'`
	echo next_root=$next_root
elif [ ${root_part: -1} = "3" ]
then
	echo "part_three"
	next_root=`echo $root_part | sed  's/.$/2/'`
	echo next_root=$next_root
fi

# Get PARTUUID of the partitions 
root_uuid=`blkid $root_part | awk -F "PARTUUID=" '{print $NF;}' | cut -d'"' -f2`
download_uuid=`blkid $download_part | awk -F "PARTUUID=" '{print $NF;}' | cut -d'"' -f2`
next_root_uuid=`blkid $next_root | awk -F "PARTUUID=" '{print $NF;}' | cut -d'"' -f2`
echo root_uuid=$root_uuid
echo next_root_uuid=$next_root_uuid
echo download_uuid=$download_uuid

# Remove the old entries of fstab, of next_root
egrep -v "($root_uuid|$download_uuid|$next_root_uuid)" /etc/fstab > /mnt/next_root/etc/_fstab && mv /mnt/next_root/etc/_fstab /mnt/next_root/etc/fstab

# Update the fstab of next_root
echo "PARTUUID=$next_root_uuid	/	ext4	defaults,noatime	0	1" >> /mnt/next_root/etc/fstab
mkdir -pv /mnt/next_root
echo "PARTUUID=$root_uuid	/mnt/next_root	ext4	defaults,noatime	0	2" >> /mnt/next_root/etc/fstab
mkdir -pv /mnt/download
echo "PARTUUID=$download_uuid	/mnt/download	ext4	defaults,noatime	0	2" >> /mnt/next_root/etc/fstab

## Step 2: Copying WiFi config. files to the next rootfs
cp /etc/wpa_supplicant/wpa_supplicant.conf /mnt/next_root/etc/wpa_supplicant/

## Step 3: Add uplink and other scripts to systemd
# Add uplink to systemd
cp /mnt/download/systemd/uplink.service /mnt/next_root/etc/systemd/system/
if [ -f /mnt/next_root/etc/systemd/system/multi-user.target.wants/uplink.service ]
then
	unlink  /mnt/next_root/etc/systemd/system/multi-user.target.wants/uplink.service 
fi
ln -s /mnt/next_root/etc/systemd/system/uplink.service /mnt/next_root/etc/systemd/system/multi-user.target.wants/uplink.service

# Bridge app maynot be needed at systemd
"""
cp /mnt/download/systemd/bridge.service /mnt/next_root/etc/systemd/system/
if [ -f /mnt/next_root/etc/systemd/system/multi-user.target.wants/bridge.service ]
then
	unlink  /mnt/next_root/etc/systemd/system/multi-user.target.wants/bridge.service 
fi
ln -s /mnt/next_root/etc/systemd/system/bridge.service /mnt/next_root/etc/systemd/system/multi-user.target.wants/bridge.service
"""
# Add startup script to systemd
cp /mnt/download/systemd/startup.service /mnt/next_root/etc/systemd/system/
if [ -f /mnt/next_root/etc/systemd/system/multi-user.target.wants/startup.service ]
then
	unlink  /mnt/next_root/etc/systemd/system/multi-user.target.wants/startup.service 
fi
ln -s /mnt/next_root/etc/systemd/system/startup.service /mnt/next_root/etc/systemd/system/multi-user.target.wants/startup.service

## Step 4: Reboot the system
TWO_OK=/boot/two_ok
TWO_BOOT=/boot/two
TWO_DOWNLOAD=/mnt/download/two
THREE_OK=/boot/three_ok
THREE_BOOT=/boot/three
THREE_DOWNLOAD=/mnt/download/three

root_part=`awk -F"root=" '{ print $NF; }' /proc/cmdline | cut -d" " -f1`

if [ ${root_part: -1} = "2" ]
then
	if [ -f "$TWO_BOOT" ]
	then
		rm -rf $TWO_BOOT
	fi
	if [ -f "$TWO_OK" ]
	then
		rm -rf $TWO_OK
	fi
	if [ -f "$TWO_DOWNLOAD" ]
	then
		rm -rf $TWO_DOWNLOAD
	fi
	touch $THREE_DOWNLOAD
	touch $THREE_BOOT
elif [ ${root_part: -1} = "3" ]
then
	if [ -f "$THREE_BOOT" ]
	then
		rm -rf $THREE_BOOT
	fi
	if [ -f "$THREE_OK" ]
	then
		rm -rf $THREE_OK
	fi
	if [ -f "$THREE_DOWNLOAD" ]
	then
		rm -rf $THREE_DOWNLOAD
	fi
	touch $TWO_DOWNLOAD
	touch $TWO_BOOT
fi

sudo reboot
