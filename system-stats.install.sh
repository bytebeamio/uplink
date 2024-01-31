#!/bin/sh

# Find link to download latest release of uplink
BIN_URL=$(curl -H "Accept: application/vnd.github.v3+json" -s https://api.github.com/repos/bytebeamio/uplink/releases/latest | grep "system-stats" | grep "download_url" | cut -d : -f 2,3 | tr -d \" )

echo "Downloading system-stats"
echo "url: ${BIN_URL}"
curl -SfL -o "/bin/system-stats" ${BIN_URL}
chmod +x "/bin/system-stats"