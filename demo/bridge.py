import socket
import json
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5555))

while True:
    recv = json.loads(s.recv(2048))
    print(recv)
    t = int(time.time()*1000000)
    p = {
        "stream": "action_status",
        "sequence": 0,
        "timestamp": t,
        "payload": {
            "id": recv["action_id"],
            "timestamp": t,
            "state": "Completed",
            "progress": 100,
            "errors": []
        }
    }
    print(p)
    time.sleep(5)
    s.sendall(bytes(json.dumps(p), encoding="utf-8"))
