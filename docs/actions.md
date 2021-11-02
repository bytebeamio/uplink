# Action
An `Action` is the term used to refer to messages that carry commands and other information that can be used by uplink or devices connected to it. Some common Actions include the `firmware_update` and `config_update` actions which when executed by the target device will lead to the initiation of an OTA update. A firmware update `Action` messages in JSON would be structured as follows:
```js
{
    "action_id": "1234",        // An integer value that can be used to maintain indempotence
    "kind": "process",          // May hold values such as control, process, depending on end-use
    "name": "update_firmware",  // Name given to Action
    "payload": "{}"             // Contains JSON data including URL to OTA update file to be downloaded
}
```

# ActionResponse
An `ActionResponse` as the name suggests, is the term used to refer to messages that carry a reply or status update from uplink or devices connected to it, used to update subscribed devices to the progress of a task/Action.
```js
{
    "action_id": "1234",            // The same as the executing Action
    "timestamp": 
    "state": "Running",             // "Running", "Completed", "Progress" or "Failed", depending on status of Action in execution
    "progress": "update_firmware",  // Useful only in case of Progress, to denote progress towards Expected Completion
    "errors": []                    // Contains a list of errors or a backtrace
}
```