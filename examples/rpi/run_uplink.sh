#!/bin/bash
# Start uplink and bridge services
systemctl enable uplink.service
systemctl enable bridge.service
systemctl start uplink.service
systemctl start bridge.service

