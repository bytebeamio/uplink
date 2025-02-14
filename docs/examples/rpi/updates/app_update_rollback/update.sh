#!/bin/bash

## Wrapper script to select appropriate update_script
# Set INSTALL_AB to yes, if app needs to be installed in both the partitions
# If the app installation fails, the previous version of the app is restored

FILE_PATH=`dirname $(readlink -f "${BASH_SOURCE:-$0}")`
SLEEP_TIME=2
INSTALL_AB="no"

## App update with rollback
APP_NAME=hello_app
APP_BIN_PATH=/usr/local/bin
$FILE_PATH/app_update_rollback.sh $1 $2 $APP_NAME $APP_BIN_PATH $FILE_PATH $INSTALL_AB $SLEEP_TIME
