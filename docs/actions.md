**Receiving Actions**:
Actions are messages that uplink expects to receive from the broker and is executable on the user's device. The JSON format for Actions are as such:
```js
{
    "action_id": "...",
    "name": "...",
    "payload": "..."
}
```
> **NOTE**: Some Actions are executed by built-in handlers, e.g. Actions. The tunshell action, i.e. Action with `name: tunshell` is used to [initiate a remote connection over tunshell](#Remote-Shell-Connection) while the OTA action, i.e. Action with `name: update_firmware` can [download OTA updates](#Downloading-OTA-updates).

**Responding with Action Responses**:
Applications can use Action Response messages to update uplink on the progress of an executing Action. They usually contain information such as a progress counter and error backtrace. Action Responses are handled as Streamed data payloads in the "action_status" stream and thus have to be enclosed as such. uplink expects Action Responses to have the following JSON format:
```js
{
    "stream": "action_status",
    "sequence": ...,
    "timestamp": ...,
    "action_id": "...",
    "state": "...",
    "progress": ...,
    "errors": [...]
}
```

> **NOTE**: Connected application should forward all response events as JSON text, following ASCII newline encoding.

An example success response to an action with the id `"123"`, would look like:
```js
{
    "stream": "action_status",
    "sequence": 234,
    "timestamp": 192323,
    "action_id": "123",
    "state": "Completed",
    "progress": 100,
    "errors": []
}
```

#### Downloading OTA updates and other files
uplink has a built-in feature that enables it to download OTA firmware updates, this can be enabled by setting the following field in the `config.toml` and using the `-c` option while starting uplink:
```toml
[downloader]
actions = ["update_firmware"]
path = "/path/to/directory" # Where you want the update file to be downloaded
```
```sh
uplink -c config.toml -a auth.json
```

Once enabled, Actions with the following JSON will trigger uplink to download the file and hand-over the action to a connected application to perform the necessary firmware updation:
```js
{
    "action_id": "...",
    "name": "update_firmware",
    "payload": "{
        \"url\": \"https://example.com/file\",
        \"version\":\"1.0\"
    }"
}
```
Once downloded, the payload JSON is updated with the file's on device path, as such:
```js
{
    "action_id": "...",
    "name": "update_firmware",
    "payload": "{
        \"url\": \"https://example.com/file\",
        \"download_path\": \"/path/to/directory\",
        \"version\":\"1.0\"
    }"
}
```

### Testing with netcat

You can test sending JSON data to Bytebeam over uplink with the following command while uplink is active

```sh
nc localhost 5050
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
{ "stream": "can", "sequence": 1, "timestamp": 12345, "data": 100 }
```

The complete API reference for the uplink library is available within the [library documentation][docs.rs].

