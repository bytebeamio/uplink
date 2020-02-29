#!/bin/sh
arm-oe-linux-gnueabi-gcc  -march=armv7-a -mfloat-abi=softfp -mfpu=neon --sysroot=/home/tekjar/Workspace/ec25/EC25EFAR06A01M4G_OCPU_01.001.01.001_SDK/ql-ol-sdk/ql-ol-crosstool/sysroots/armv7a-vfp-neon-oe-linux-gnueabi $@
