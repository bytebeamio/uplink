#!/bin/bash

## Wrapper script to select appropriate update_script
# Set INSTALL_AB to yes, if the app needs to be installed in both the partitions

FILE_PATH=`dirname $(readlink -f "${BASH_SOURCE:-$0}")`
SLEEP_TIME=2

## App update without rollback
APP_NAME=hello_app
APP_BIN_PATH=/usr/local/bin
INSTALL_AB="no"
$FILE_PATH/app_update.sh $1 $2 $APP_NAME $APP_BIN_PATH $FILE_PATH $INSTALL_AB $SLEEP_TIME
