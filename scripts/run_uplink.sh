#!/bin/bash
# Start uplink and bridge services
systemctl enable uplink.service
systemctl enable bridge.service
systemctl enable startup.service
systemctl start uplink.service
systemctl start bridge.service
systemctl start startup.service

