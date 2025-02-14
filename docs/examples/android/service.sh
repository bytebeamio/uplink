#!/system/bin/sh

set -x

export MODULE_DIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
. $MODULE_DIR/env.sh
mkdir -p $DATA_DIR

cd $DATA_DIR || exit

$MODULE_DIR/bin/uplink -a $DATA_DIR/device.json -c $MODULE_DIR/etc/uplink.config.toml $UPLINK_LOG_LEVEL 2>&1 | $MODULE_DIR/bin/logrotate --max-size 10000000 --backup-files-count 30 --output $DATA_DIR/out.log