# Usage
Pipe STDOUT of app to stdout-collector, provide arguments to connect to uplink instance including port number and the stream onto which data should be pushed onto
```
$APP_WRITING_TO_STDOUT | stdout-collector -p 5555
 -s stream_name
```