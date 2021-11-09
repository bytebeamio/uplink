# Actions
uplink handles device data, `Action`s and `ActionResponse`s. An action is received from the broker and sent to user apps that can also send either user data or action responses, containing status of actions in execution.

## Action
An `Action` is the term used to refer to messages that carry commands and other information that can be used by uplink or apps connected to it. Some common Actions include the `firmware_update` and `config_update` actions which when executed by the target device will lead to the initiation of an OTA update. A firmware update `Action` messages in JSON would be structured as follows:
```js
{
    "action_id": "1234",        // An integer value that can be used to maintain indempotence
    "kind": "process",          // May hold values such as control, process, depending on end-use
    "name": "update_firmware",  // Name given to Action
    "payload": "{}"             // Contains JSON data including URL to OTA update file to be downloaded
}
```

## Action Response
The app can send back progress updates for an Action by publishing an `ActionResponse` message to the action_status stream
```js
{
    "action_id": "1234",            // The same as the executing Action
    "timestamp": 1234567890000,     // Timestamp at response generation, unsigned 64bit integer value
    "state": "Running",             // "Running", "Completed", "Progress" or "Failed", depending on status of Action in execution
    "progress": "update_firmware",  // Useful only in case of Progress, to denote progress towards Expected Completion
    "errors": []                    // Contains a list of errors or a backtrace
}
```

## Demonstration
We have provided examples written in python and golang to demonstrate how you can receive Actions and reply back with either data or responses. You can checkout the examples provided in the `demo/` directory and execute them as such:
1. Ensure uplink is running on device and connected to relevant broker.
2. Run the python/golang examples
```sh
# For python
python demo/app.py
# For golang
go run demo/app.go
```
3. Create and send an action targeted at device(using relevant topic on broker).
4. Monitor how action status is received as response on action_status topic.