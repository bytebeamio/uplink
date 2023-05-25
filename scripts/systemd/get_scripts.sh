#!/bin/bash
if [ ! -f /mnt/download/one_time_setup_done ]
then
	curl  --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/sai-kiran-y/uplink/main/scripts/one_time_setup.sh | bash
	touch /boot/two
fi
