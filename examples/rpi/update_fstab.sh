#!/bin/bash
root_part=`awk -F"root=" '{ print $NF; }' /proc/cmdline | cut -d" " -f1`
download_part=`echo $root_part | sed 's/.$/4/'`

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

root_uuid=`blkid $root_part | awk -F "PARTUUID=" '{print $NF;}' | cut -d'"' -f2`
download_uuid=`blkid $download_part | awk -F "PARTUUID=" '{print $NF;}' | cut -d'"' -f2`
next_root_uuid=`blkid $next_root | awk -F "PARTUUID=" '{print $NF;}' | cut -d'"' -f2`
echo root_uuid=$root_uuid
echo next_root_uuid=$next_root_uuid
echo download_uuid=$download_uuid

egrep -v "($root_uuid|$download_uuid|$next_root_uuid)" /etc/fstab > /etc/_fstab && mv /etc/_fstab /etc/fstab

echo "PARTUUID=$root_uuid	/	ext4	defaults,noatime	0	1" >> /etc/fstab

mkdir -pv /mnt/next_root
echo "PARTUUID=$next_root_uuid	/mnt/next_root	ext4	defaults,noatime	0	2" >> /etc/fstab

mkdir -pv /mnt/download
echo "PARTUUID=$download_uuid	/mnt/download	ext4	defaults,noatime	0	2" >> /etc/fstab

mount -a
