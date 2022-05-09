#!/bin/sh
TARGETS=$@
BUILD_DIR="build/"

for TARGET in ${TARGETS}
do
    echo "Building uplink for ${TARGET}"
    cross build --release --target ${TARGET}
done

echo "Creating directory to store built executables: ${BUILD_DIR}"
mkdir -p ${BUILD_DIR}
for TARGET in ${TARGETS}
do
    echo "uplink binary for ${TARGET} copied into build/ folder"
    cp target/${TARGET}/release/uplink ${BUILD_DIR}uplink-${TARGET}
done
