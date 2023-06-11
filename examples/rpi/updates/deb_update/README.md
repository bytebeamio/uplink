# Deb update

If you have a deb package to be installed on the device(Raspberry Pi in this case),
then deb_update is the right way do it. In this example, we try to install the apt-src deb 
package on the Raspberry Pi. [deb_update.sh](deb_update.sh) script has the logic of 
installing a deb package.

## updater
[updater](updater) is the wrapper script for the deb_update.sh script. It calls
the deb_update.sh script with parameters such as file path, port number and so on.
The updater script and uplink communicate through this specified port.

## Create update file
All these files including the updater, deb_update.sh, deb package needs to be compressed
in tar.gz format. Running make_firmware_update.sh script creates the deb_update.tar.gz file.
This can be uploaded as a new firmware on Bytebeam cloud.

## Status of the update
Once the OTA update is triggered on the cloud, the deb_update.tar.gz file is downloaded on
the device and it's contents are extracted and updater is run. This calls the deb_update
script with appropriate parameters. Once the deb package is installed, the status "Completed"
is sent to the cloud. In the Bytebeam cloud, this particular update will be marked as Completed.
