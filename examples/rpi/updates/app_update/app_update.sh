#!/bin/bash

## This script updates the user application that is running as a service 
## Summary:
##			1. Stop the service related to the app
##			2. Replace the application binary in both A, B partitions if INSTALL_AB is set to yes
## 			3. Restart the application service
##			4. Check if service is running as expected.
##			5. Send the status(success/failure) to uplink

# COPROC[1] is the stdin for netcat
# COPROC[0] is the stdout of netcat
# By echoing to the stdin of nc, we write to the port 5050

PORT=$2
APP=$3
APP_BIN_PATH=$4
FILE_PATH=$5
INSTALL_AB=$6
SLEEP_TIME=$7

coproc nc localhost $PORT

if [ -f /etc/systemd/system/$APP.service ]
then
	# Stop the service
	systemctl kill $APP

	cp $FILE_PATH/$APP $APP_BIN_PATH/

	# Restart the service
	systemctl start $APP
	sleep $SLEEP_TIME
	
	# Check the status of the service
	if [ "$(systemctl is-active $APP.service)" = "active" ]
	then
		echo "is active"
		# Send status(success) to uplink
		echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\": \"$1\", \"state\": \"Completed\", \"progress\": 100, \"errors\": [] }" >&"${COPROC[1]}"

		# Update the other partition also if INSTALL_AB is set to yes
		if [ "$INSTALL_AB" = "yes" ]
		then
			cp $FILE_PATH/$APP /mnt/next_root/$APP_BIN_PATH/
		fi
	else
		echo "inactive"
		# Send status(failed) to uplink
		echo "{ \"stream\": \"action_status\", \"sequence\": 0, \"timestamp\": $(date +%s%3N), \"action_id\": \"$1\", \"state\": \"Failed\", \"progress\": 100, \"errors\": [\"Could not install the app\"] }" >&"${COPROC[1]}"
	fi
else
	echo "$APP.service does not exist"
fi
