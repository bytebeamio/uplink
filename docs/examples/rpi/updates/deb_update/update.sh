#!/bin/bash

## Wrapper script to select appropriate update_script

FILE_PATH=`dirname $(readlink -f "${BASH_SOURCE:-$0}")`

## Deb update
$FILE_PATH/deb_update.sh $1 $2 $FILE_PATH $INSTALL_PATH
