#!/bin/bash
## This script extracts the rootfs to the new partition
## Steps:
#			1. Extract the rootfs to new partition
#			2. Update the fstab
#			3. Copy WiFi config. files to new rootfs
#			4. Add uplink and startup scripts to systemd
#			5. Reboot the system

# COPROC[1] is the stdin for netcat
# COPROC[0] is the stdout of netcat
# By echoing to the stdin of nc, we write to the port 5555

PORT=$2
coproc nc localhost $PORT 

action_id=$1
echo $action_id > /mnt/download/action_id

## Step 1: Extracting the rootfs
rm -rf /mnt/next_root/*
tar -xvpzf $3/*.tar.gz -C /mnt/next_root/
echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\": \"$1\", \"state\": \"Completed\", \"progress\": 20, \"errors\": [] }" >&"${COPROC[1]}"

## Step 2: Update the fstab of extracted rootfs
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
echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\": \"$1\", \"state\": \"Completed\", \"progress\": 40, \"errors\": [] }" >&"${COPROC[1]}"

## Step 3: Copying WiFi config. files to the next rootfs
cp /etc/wpa_supplicant/wpa_supplicant.conf /mnt/next_root/etc/wpa_supplicant/
echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\": \"$1\", \"state\": \"Completed\", \"progress\": 50, \"errors\": [] }" >&"${COPROC[1]}"

## Step 4: Add uplink and other scripts to systemd
# Add uplink to systemd
cp /mnt/download/systemd/uplink.service /mnt/next_root/etc/systemd/system/
if [ -f /mnt/next_root/etc/systemd/system/multi-user.target.wants/uplink.service ]
then
	unlink  /mnt/next_root/etc/systemd/system/multi-user.target.wants/uplink.service 
fi
ln -s /mnt/next_root/etc/systemd/system/uplink.service /mnt/next_root/etc/systemd/system/multi-user.target.wants/uplink.service

# Add startup script to systemd
# startup script checks if:
#			system booted to correct partition 
#			uplink is running as expected
cp /mnt/download/systemd/startup.service /mnt/next_root/etc/systemd/system/
if [ -f /mnt/next_root/etc/systemd/system/multi-user.target.wants/startup.service ]
then
	unlink  /mnt/next_root/etc/systemd/system/multi-user.target.wants/startup.service 
fi
ln -s /mnt/next_root/etc/systemd/system/startup.service /mnt/next_root/etc/systemd/system/multi-user.target.wants/startup.service
echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\": \"$1\", \"state\": \"Completed\", \"progress\": 75, \"errors\": [] }" >&"${COPROC[1]}"

## Step 4: Reboot the system
# Before rebooting, some flags are created in data partition
# to know if reboot is due to rootfs issue or if it's a normal reboot.
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

echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\": \"$1\", \"state\": \"Completed\", \"progress\": 90, \"errors\": [] }" >&"${COPROC[1]}"

#mkdir /mnt/next_root/uboot
cp /boot/u-boot.bin /mnt/download
cp /boot/boot.scr /mnt/download

# Delete flags in boot folder of next rootfs
files=( two two_ok three three_ok )
for i in "${files[@]}"
do
	echo "$i"
	rm -rf /tmp/$i
done

# Copy the kernel and firmware files to boot partition
cp -r /mnt/next_root/boot/* /boot/

# Extract the kernel - Needed for u-boot v2022. Currently v2023 is being used.
# cp /mnt/next_root/boot/kernel8.img /mnt/next_root/boot/_kernel8.img.gz
# gunzip /mnt/next_root/boot/_kernel8.img.gz

# Update config file to load uboot 
# echo "kernel=u-boot.bin">>/boot/config.txt

# Place uboot script in boot partition
cp /mnt/download/boot.scr /boot/
cp /mnt/download/u-boot.bin /boot/

# Create symlink between the contents of boot folder and uboot folder
"""
BOOT_PATH="/mnt/next_root/boot/*"
UBOOT_PATH="/uboot"
for FILE in  $BOOT_PATH;
do
	if [ -d "$FILE" ]
	then
		for SUBFILE in `ls $FILE`
		do
			if [ -f $UBOOT_PATH/$(basename $FILE)/$SUBFILE ]
			then
				ln -sf $UBOOT_PATH/$(basename $FILE)/$SUBFILE $FILE/$SUBFILE
			fi
		done
	fi
	if [ -f $UBOOT_PATH/$(basename $FILE) ]
	then
		ln -sf $UBOOT_PATH/$(basename $FILE) $FILE 
	fi
done
"""
# If the boot is successful, startup script sends progress as 100.
sudo reboot
