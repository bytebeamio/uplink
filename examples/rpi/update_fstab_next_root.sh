#!/bin/bash
root_part=`awk -F"root=" '{ print $NF; }' /proc/cmdline | cut -d" " -f1`
download_part=`echo $root_part | sed 's/.$/4/'`
echo root_part=$root_part
echo download_part=$download_part

echo ${root_part: -1}

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

egrep -v "($root_uuid|$download_uuid|$next_root_uuid)" /etc/fstab > /mnt/next_root/etc/_fstab && mv /mnt/next_root/etc/_fstab /mnt/next_root/etc/fstab

echo "PARTUUID=$next_root_uuid	/	ext4	defaults,noatime	0	1" >> /mnt/next_root/etc/fstab

mkdir -pv /mnt/next_root
echo "PARTUUID=$root_uuid	/mnt/next_root	ext4	defaults,noatime	0	2" >> /mnt/next_root/etc/fstab

mkdir -pv /mnt/download
echo "PARTUUID=$download_uuid	/mnt/download	ext4	defaults,noatime	0	2" >> /mnt/next_root/etc/fstab

<<com
if [ $(grep -L "$next_root_uuid" /etc/fstab) ]
then
	echo "/mnt/next_root not present"
	mkdir -pv /mnt/next_root
	echo "PARTUUID=$next_root_uuid	/mnt/next_root	ext4	defaults,noatime	0	2" >> /etc/fstab
else
	echo "/mnt/next_root already present"
fi

if [ $(grep -L "$download_uuid" /etc/fstab) ]
then
	echo "/mnt/download not present"
	mkdir -pv /mnt/download
	echo "PARTUUID=$download_uuid	/mnt/download	ext4	defaults,noatime	0	2" >> /etc/fstab
else
	echo "/mnt/download already present"
fi

tmp=`cat /etc/fstab | grep -e "$download_uuid"`
echo var=$tmp
if [ -z "$tmp" ]
then
	echo empty
	mkdir -pv /mnt/download
	echo "PARTUUID=$next_root_uuid	/mnt/download	ext4	defaults,noatime	0	2" >> /etc/fstab
fi

var=`cat /etc/fstab | grep -e "$next_root" -e "$next_root_uuid"`
echo var=$var
if [ -z "$var" ]
then
	echo empty
	mkdir -pv /mnt/next_root
	echo "PARTUUID=$next_root_uuid	/mnt/next_root	ext4	defaults,noatime	0	2" >> /etc/fstab
else
	echo not_empty
fi
com

#mount -a

<<com
if cat /etc/fstab | grep -q -e $next_root -e $next_root_uuid
then
fi
if [ ${root_uuid: -1} = "2" ]
then
	echo "part_two"
	next_root_uuid=`echo $root_uuid | sed  's/.$/3/'`
	echo $next_root_uuid

elif [ ${root_uuid: -1} = "3" ]
then
	echo "part_three"
	next_root_uuid=`echo $root_uuid | sed  's/.$/2/'`
	echo $next_root_uuid
fi
com
