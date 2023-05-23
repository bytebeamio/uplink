#!/bin/bash

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

# Copying WiFi config. files to the next rootfs
cp /etc/wpa_supplicant/wpa_supplicant.conf /mnt/next_root/etc/wpa_supplicant/
