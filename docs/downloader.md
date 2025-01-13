# File Downloader
Uplink consists of a file downloader, a built-in application that can perform the initial stage of any OTA, downloading the update onto the device.

## Configuring the Downloader
As described in the [example `config.toml`](https://github.com/bytebeamio/uplink/blob/513f4c2def843f3c27bf0a7dc993667010944535/configs/config.toml#L135C1-L145C27), uplink will handle a special class of actions as [Download Actions](#download-actions) and download the associated files into the mentioned `path` within a directory with the same name as the download action(i.e. `/var/tmp/ota-file/update_firmware`). 
Here is one more example config to illustrate how uplink can be configured:
```
[downloader]
actions = [{ name = "update_firmware }]
path = "/data/local/downloads" // downloads the file into a location commonly seen in Android systems, i.e. /data/local/downloads/update_firmware
```

## Download Actions
Download actions contain a payload in JSON text format:
```
{
    "url": "https://firmware...", // URL to download file from
    "content-length": 123456, // Size of file in bytes
    "file-name": "example.txt", // Name of the file post download
    "checksum": "abc123...", // Checksum that can be used to verify the download was successful
    ...
}
```

P.S. [Bytebeam OTA updates](https://bytebeam.io/docs/triggering-ota-update) are an example of Download Action
