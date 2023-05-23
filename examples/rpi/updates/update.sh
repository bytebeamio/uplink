#!/bin/bash

## Wrapper script to select appropriate update_script

#echo "Action id received from uplink is $1 and port is $2"

FILE_PATH=`dirname $(readlink -f "${BASH_SOURCE:-$0}")`

:'
## App update without rollback
APP_NAME=app
APP_BIN_PATH=/usr/local/bin
$FILE_PATH/app_update.sh $1 $2 $APP_NAME $APP_BIN_PATH $FILE_PATH
'

## App update with rollback
:'
APP_NAME=my_app
APP_BIN_PATH=/usr/local/bin
$FILE_PATH/app_update_rollback.sh $1 $2 $APP_NAME $APP_BIN_PATH $FILE_PATH
'

## Deb update
:'
$FILE_PATH/deb_update.sh $1 $2 $FILE_PATH
'

## rootfs update
:'
$FILE_PATH/rootfs_update.sh $1 $2 $FILE_PATH
'
