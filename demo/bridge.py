import socket
import json
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5555))

while True:
    r = s.recv(2048)
    if not r:
        break
    recv = json.loads(r)
    print("Received:\n",recv)
    t = int(time.time()*1000000)
    # Wait for 5s
    time.sleep(5)
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
    print("Replying:\n", p)
    s.sendall(bytes(json.dumps(p)+"\n", encoding="utf-8"))
