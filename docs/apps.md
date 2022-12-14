# User Applications
uplink handles device data, `Action`s and `ActionResponse`s. An action is received from the broker and sent to user apps that can also send either user data or action responses, containing status of actions in execution.

## Action
An `Action` is the term used to refer to messages that carry commands and other information that can be used by uplink or apps connected to it. Some common Actions include the `update_firmware` and `config_update` actions which when executed by the target device will lead to the initiation of an OTA update. A firmware update `Action` messages in JSON would be structured as follows:
```js
{
    "action_id": "...", // An integer value that can be used to maintain indempotence
    "kind": "...",      // May hold values such as process, depending on end-use
    "name": "...",      // Name given to Action
    "payload": "..."    // Can contain JSON formatted data as a string
}
```

## Streamed Data
Connected application can send data to the broker as Streamed Payload. Streams enable uplink to send large amounts of data together, packaged as a single message. An example Streamed Payload has the following JSON format:
```js
{
    "stream": "...",  // Name of stream to which data is being sent
    "sequence": ...,  // Sequence number of data packet
    "timestamp": ..., // Timestamp at message generation
    //...payload: more JSON data
}
```

An example data packet on the stream `"location"`, with the fields `"city"` being a string and `"altitude"` being a number would look like:
```js
{
    "stream": "location",
    "sequence": 10000000,
    "timestamp": 1987654,
    "city": "Bengaluru",
    "altitude": 123456,
}
```

## Action Response
Connected user applications can send back progress updates for an Action by publishing an `ActionResponse` message to the `"action_status"` stream, where uplink immediately forwards the update, given their low frequency.
```js
{
    "stream": "action_status",  // Action Responses are to be sent to the "action_status" stream
    "sequence": ...,            // Sequence number, incremented for each new response to an action, starting from 1
    "timestamp": ...,           // Timestamp at response generation, unsigned 64bit integer value
    "action_id": "...",         // The same as the executing Action
    "state": "...",             // "Running", "Completed", "Progress" or "Failed", depending on status of Action in execution
    "progress": ...,            // Denote progress towards Expected Completion, out of 0..100
    "errors": [...]             // Contains a list of errors or a backtrace
}
```

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
