# Updates

## Uploading the update to Bytebeam Cloud
From the Bytebeam cloud, updates can be easily installed in few simple steps.
The update file is typically a tar archive(update.tar.gz) with a script(called updater) and
other files. 
These are the types of updates that are currently supported.
* [**app_update**](app_update/) -  Installs the application binary to the specified location
* [**app_update_with_rollback**](app_update_with_rollback/) - Installs the application binary to the specified location
		     					      and if the installation fails, script switches back to the
						              previous working version of the app
* [**deb_update**](deb_update/) - Installs the provided deb package to the current partition
* [**rootfs_update**](rootfs_update/) - Installs the rootfs in the other partition and switches to the new rootfs on reboot.

Each of these folders has details about creating appropriate update tar archive, that needs to be uploaded on Bytebeam cloud. 
Refer the ["Create New Firmware version"](https://bytebeam.io/docs/creating-new-firmware-version) section to understand how to upload new fimware to Bytebeam cloud.
Now the uploaded firmware, should be visible in "Firmware Versions" section of Bytebeam cloud.

## Install updates
Refer the ["Triggering OTA"](https://bytebeam.io/docs/triggering-ota-update) guide to understand how to install the update on the device from the cloud.
Once this is done, the update tar archive is downloaded and the contents are extracted on the device.
Then update.sh script is run. This script has details on how to install the updates on the device. Refer to the update folders, to get a better idea on each of the updates.
