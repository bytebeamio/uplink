#!/bin/bash
root_part=`awk -F"root=" '{ print $NF; }' /proc/cmdline | cut -d" " -f1`

coproc nc localhost 5050

if [ -f "/mnt/download/action_id" ]
then
	action_id=`cat /mnt/download/action_id`
else
	action_id=0
fi

TWO_OK=/boot/two_ok
TWO_BOOT=/boot/two
TWO_FAILED=/boot/two_failed
TWO_DOWNLOAD=/mnt/download/two
THREE_OK=/boot/three_ok
THREE_BOOT=/boot/three
THREE_FAILED=/boot/three_failed
THREE_DOWNLOAD=/mnt/download/three

if [ -f $TWO_FAILED ]
then
   rm -rf $TWO_FAILED
fi

if [ -f $THREE_FAILED ]
then
   rm -rf $THREE_FAILED
fi

if [ -f "$THREE_DOWNLOAD" ]
then
	if [ ${root_part: -1} = "2" ]
	then
		# Boot failed
		rm -rf $THREE_BOOT
		rm -rf $THREE_FAILED 
		touch $TWO_BOOT
		touch $TWO_DOWNLOAD
		if [ -e $THREE_DOWNLOAD ]
		then
			rm -rf $THREE_DOWNLOAD
		fi

		# Notify uplink
		if [ "$action_id" != 0 ]
		then
			echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\":\"$action_id\", \"state\": \"Failed\", \"progress\": 100, \"errors\": [] }" >&"${COPROC[1]}"
			echo 0 > /mnt/download/action_id
		fi
	elif [ ${root_part: -1} = "3" ]
	then
		# Boot successful
		rm -rf $THREE_FAILED
		touch $THREE_OK
		if [ "$action_id" != 0 ]
		then
			echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\":\"$action_id\", \"state\": \"Completed\", \"progress\": 100, \"errors\": [] }" >&"${COPROC[1]}"
		fi
	fi
elif [ -f "$TWO_DOWNLOAD" ]
then
	if [ ${root_part: -1} = "3" ]
	then
		# Boot failed
		rm -rf $TWO_BOOT
		rm -rf $TWO_FAILED
		touch $THREE_BOOT
		touch $THREE_DOWNLOAD
		if [ -f $TWO_DOWNLOAD ]
		then
			rm -rf $TWO_DOWNLOAD
		fi

		# Notify uplink
		if [ "$action_id" != 0 ]
		then
			echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\": \"$action_id\", \"state\": \"Failed\", \"progress\": 100, \"errors\": [] }" >&"${COPROC[1]}"
		fi
	elif [ ${root_part: -1} = "2" ]
	then
		# Boot successful
		if [ "$action_id" != 0 ]
		then
			echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\":\"$action_id\", \"state\": \"Completed\", \"progress\": 100, \"errors\": [] }" >&"${COPROC[1]}"
			echo 0 > /mnt/download/action_id
		fi
		rm -rf $TWO_FAILED
		touch $TWO_OK
	fi
fi
