# User Applications
uplink is a service that runs in the background and connects user applications to the bytebeam platform. 

## Configuring uplink
Applications can connect to the uplink service over TCP to send and receive JSON data, and for this uplink has to expose a TCP port per application. The following example configuration describes to uplink that two applications require it to expose the ports 5555 and 6666, where one of them also expects to receive `install_firmware` actions:
```
[tcpapps.main_app]
port = 5555

[tcpapps.ota_installer]
port = 5555
actions = [{ name = "install_firmware" }]
```
NOTE: Only one client can connect to a TCP port at a time. If a second client attempts to connect to a port which is already occupied, the first client will be disconnected from uplink.

## Action
An `Action` is the term used to refer to messages that carry commands and other information that can be executed by uplink or applications connected to it. Some common actions include the `update_firmware` and `config_update`. An `Action` messages in JSON would be structured as follows:
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
    //...payload: stream columns as JSON key+value pairs
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
Connected user applications can send back progress updates for an action in execution by publishing an `ActionResponse` message to the `"action_status"` stream.
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
> **NOTE:** uplink immediately forwards the update, given their low frequency.

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

> **NOTE:** There is a timeout mechanism which on being triggered will send a ***Failed*** response to platform and stop forwarding any *Progress* responses from the connected applications. In order to not trigger this timeout, an application must send a ***Failed*** or ***Completed*** response before the action timeout. Once an action has timedout, a failure response is sent and all it's future responses are dropped. Action timeouts can be configured per action when setting up uplink, as follows:
> ```
> [tcpapps.main_app]
> port = 5555
> actions = [{ name = "abc", timeout = 300 }] # Allow the connected app to send responses for action abc upto 5 minutes from receive, send a failure response and drop all responses afterwards if not yet completed.
> ```

## Demonstration
We have provided examples written in python and golang to demonstrate how you can receive Actions and reply back with either data or responses. You can checkout the examples provided in the `demo/` directory and execute them as such:
1. Ensure uplink is running on the device, connected to relevant broker and using the following config:
```toml
[tcpapps.main_app]
port = 5555
actions = [{ name = "update_firmware" }, { name = "reboot" }, { name = "update_config" }]
```
2. Run the python/golang examples
```sh
# For python
python demo/app.py
# For golang
go run demo/app.go
```
3. Create and send an action targeted at device(using relevant topic on broker).
4. Monitor how action status is received as response on action_status topic.
