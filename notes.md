
## REPO TOUR

`uplink/base` -> all uplink related stuffs.

`uplink/collector` -> contains built-in applications

### 2 types of app in uplink-

1. doing stuffs using actions (actions are commands that are send by the platform)

2. collecting data from telemetrics data (eg from sensors) CAN

### Uplink Base 

- **Bridge**: There are 2 types of lanes in bridge in uplink. `Actions lane` and `Data lane`, respectively. **Actions lane** is used to receive and respond to actions. Whereas **Data lane** is used only to forward or send data.

### Duties of `Action lane`:

- Guard against new actions when there exists another action in execution.

- Receive responses, from the connected application and forwards it if the currently executing action has the same action id.

- If action is not configured, uplink will reject it.

- Sensor recieves responses, [when an action is received by Uplink, it is sending the response that it receives the action]

- Performs timeout of actions that are running or executing for more time than specified or configured.

### Duties of `Data lane`:

- it receives data as `JSON object`, it takes that data object and batches it upto batch size [batch size is customizable]

- separate data into streams

- If a particular stream has not reached batch size, due to slow data generation or any other reason, push the data after a timeout as configured by the user.

### Response types:

- Receive [uplink sends when an action is received through the mqtt connection]

- Running [contains progress information from the application] (possible values from 0 to 100) of 1 byte max

- Completed [sent on completing execution of action]

- Failed [sent when an action can't be/fails to be executed, we can also send backtrace data along with the failed response]

## Platform Behaviour

Resends the action when there is no response.


## Serializer

Writes data into some place that it can read out of later.

User can customize serializer for certain streams.

eg. - 

If lots of data is expected on that stream and all of it is important, user can configure the serializer to write it to the disc.

If not much data is expected but all of it is still important then we keep in memory (RAM only).


## Todo

- We are using multi context tokio processes. [go through tokio]
- Go through concurrency & parallelism
- Go through codebase