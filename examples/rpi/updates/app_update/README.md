# App Update

App update basically replaces the existing binary on the system with the
binary uploaded on the Bytebeam cloud. In this example, the app is expected to be part of systemd. 
The app basically prints the string "Hello from Bytebeam" every 3 seconds.
[app_update.sh](app_update.sh) has the logic of replacing the old app, with the new one.
By default the app is assumed to be in the folder /usr/local/bin. This is configurable in update.sh script.

## updater
[updater](updater) is the wrapper script for app_update script. Using updater, 
we can configure several parameters such as
* Port number
* App name
* App path - Path where the app is expected to be installed
* install_ab - Whether the app needs to be installed on both the partitions

## Create app update
make_firmware_update.sh script creates the [app_update](app_update.tar.gz) tar file
which has the new application, updater and app_update.sh scripts. The app_update.tar.gz 
file is to be uploaded to the "Firmware Update" section of Bytebeam cloud. Once the OTA
update is triggered, the tar is downloaded to the device and updater is run, which
inturn calls app_update.sh with appropriate parameters. 

## Status of the update
app_update.sh replaces the old app with the new app in the tar file. Then the script checks if the 
new app runs successfully, by restarting the app service in systemd. If it's successful, the status "Completed" is sent 
to the cloud. Else, the status "Failed" is sent to the cloud. The same can be verified on the Bytebeam cloud.
