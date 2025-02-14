# OTA Updates on Raspberry Pi

## Setting up Raspberry Pi
In order to get started with the Over-the-air(OTA) updates on RaspberryPi using Bytebeam Cloud, 
an initial setup needs to be done. Download the bytebeam-rpi image, from this link and flash 
the image on the SD Card, using tools like rpi-imager. It is recommended to use rpi-imager to 
flash the image, instead of using dd command(on Linux). Wifi credentials can be set during this 
stage itself. Once the flashing is complete, insert the SD Card into the Pi and power on the Pi.
A monitor can be connected to the Pi, using HDMI port on it. The Pi can also be connected to the 
native system, via UART or via SSH.

It may be noted that for the purpose of OTA updates, the SD Card is formatted to have 3 partitions 
of ext4 format.
* The first partition is called "A Partition"
* The second partition is called "B Partition"
* The third partition is called "Data Partition"

At any given point of time either "A partition" or "B partition" will be the active rootfs partition,
and the other partition is referred to as inactive partition. Data partition is where the persistent
files are stored. During the kernel updates, the contents of inactive partition are replaced with the
new rootfs. 

## Device provisioning
Create an account on Bytebeam cloud. [This](https://bytebeam.io/docs/getting-started-on-bytebeam-cloud) guide 
helps in getting started on Bytebeam Cloud. Once the account has been created, the device(Raspberry Pi)
can be provisioned. Refer to [this](https://bytebeam.io/docs/provisioning-a-device) guide on how to provision a device.
This should download the device configuration file in JSON format. Each device has a unique config. file. 
Rename the file as device.json and place it in "/mnt/download" folder of rpi. To connect the device 
to the cloud, run the script, run_uplink.sh in /mnt/download folder. The device should be visible on the
Bytebeam cloud now. Go to [Bytebeam cloud](https://cloud.bytebeam.io/) and check if the device is shown on the UI.

## Uplink
uplink is the client, running on the device, that enables the device to connect to the Bytebeam cloud. 

## Updates
This completes the device provisioning part. Now, from the cloud, several cool features such as OTA updates,
remote shell, can be used. To know more about the OTA updates and to get some hands-on experience,
see the details in [updates](updates) folder.
