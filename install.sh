#!/bin/sh

ARCH=$(uname -m)
OS=$(uname -o)
UPLINK_DIR=~/.uplink/

case "${ARCH}, ${OS}" in
    "x86_64, GNU/Linux") TARGET="x86_64-unknown-linux-gnu"
        ;;
    "armv7, GNU/Linux") TARGET="armv7-unknown-linux-gnueabihf"
        ;;
    "armv7l, GNU/Linux") TARGET="armv7-unknown-linux-gnueabihf"
        ;;
    *) echo "Unknown target, no uplink binary available. Open an issue to add support for your platform."
       echo "https://github.com/bytebeamio/uplink/issues/new?labels=new-target&title=Add+support+for+${OS}+on+${ARCH}"; exit
        ;;
esac

echo "Creating directory to store uplink executable: ${UPLINK_DIR}"
mkdir -p ${UPLINK_DIR}

# Find link to download latest release of uplink
UPLINK_URL=$(curl -H "Accept: application/vnd.github.v3+json" -s https://api.github.com/repos/bytebeamio/uplink/releases/latest | grep ${TARGET} | grep "download_url" | cut -d : -f 2,3 | tr -d \" )

echo ""
echo "Downloading uplink for the target ${TARGET}"
echo "url: ${UPLINK_URL}"
curl -SfL -o "${UPLINK_DIR}uplink" ${UPLINK_URL}
chmod +x "${UPLINK_DIR}uplink"

echo "Add ${UPLINK_DIR} to PATH"
