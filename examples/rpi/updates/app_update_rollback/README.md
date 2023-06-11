# App update with rollback feature

This update basically replaces the existing binary on the system with the
binary uploaded on the Bytebeam cloud. In this example,the app is expected 
to be part of systemd. The app basically prints the string "Hello from Bytebeam" 
every 3 seconds. [app_update_rollback.sh](app_update_rollback.sh) has the logic of 
replacing the old app, with the new one. By default the app is assumed to be in the 
folder /usr/local/bin. This is configurable in updater script. If the new app doesn't
work as expected, it is replaced with the older working version of the app.

## updater 
[updater](updater) is the wrapper script for app_update_rollback script. Using updater 
script we can configure several parameters such as
* Port number
* App name
* App path - Path where the app is expected to be installed
* install_ab - Whether the app needs to be installed on both the partitions

## Create app update
make_firmware_update.sh script creates the [app_update_rollback](app_update_rollback.tar.gz) tar file
which has the new application, updater and app_update_rollback.sh scripts. The app_update_rollback.tar.gz 
file is to be uploaded to the "Firmware Update" section of Bytebeam cloud. Once the OTA
update is triggered, the tar is downloaded to the device and updater is run, which
inturn calls app_update_rollback.sh with appropriate parameters. 

## Status of the update
app_update_rollback.sh replaces the old app with the new app in the tar file. Then the script checks if the 
new app runs successfully, by restarting the app service in systemd. If it's successful, the status "Completed" is sent 
to the cloud. Else, the new app is replaced with the old app status "Failed" is sent to the cloud,
with the error message saying that the app rolled back to the working version.The same can be verified 
on the Bytebeam cloud.
