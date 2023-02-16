import socket
import json
import time
import os
import stat
import shutil
import threading
import sys

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
port = "500"+sys.argv[1]
print("Connecting to :", port)
while True:
    try: 
        s.connect(("localhost", int(port)))
        break
    except:
        print("Reconnecting")

# Converts JSON data received over TCP into a python dictionary
def recv_action(s):
    r = s.recv(2048)
    if not r:
        return
    return json.loads(r)

# Constructs a payload and sends it over TCP to uplink
def send_data(s, payload):
    send = json.dumps(payload) + "\n"
    s.sendall(bytes(send, encoding="utf-8"))

# Constructs a JSON `action_status` as a response to received action on completion
def action_status(id, status, progress):
    return {
        "stream": "action_status",
        "sequence": 0,
        "timestamp": int(time.time()*1000000),
        "action_id": id,
        "state": status,
        "progress": progress,
        "errors": []
    }


while True:
    action = recv_action(s)
    if not action:
        continue
    print(action)
    action_id = action["action_id"]

    for i in range(100):
        time.sleep(1)
        resp = action_status(action_id, "Installing", i)
        print(resp)
        send_data(s, resp)


    resp = action_status(action_id, "Completed", 100)
    print(resp)
    send_data(s, resp)


