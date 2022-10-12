import socket
import json
import time
import os
import stat
import shutil
import threading

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5555))

# Converts JSON data received over TCP into a python dictionary
def recv_action(s):
    return json.loads(s.recv(2048))

# Constructs a payload and sends it over TCP to uplink
def send_data(s, payload):
    send = json.dumps(payload) + "\n"
    s.sendall(bytes(send, encoding="utf-8"))

# Constructs a JSON `action_status` as a response to received action on completion
def action_complete(id):
    return {
        "stream": "action_status",
        "sequence": 0,
        "timestamp": int(time.time()*1000000),
        "id": id,
        "state": "Completed",
        "progress": 100,
        "errors": []
    }

def update_firmware(action):
    payload = json.loads(action['payload'])
    print(payload)
    shutil.move(payload["ota_path"], "/tmp/foobar")
    os.chmod("/tmp/foobar", 0o755)

def recv_actions():
    while True:
        action = recv_action(s)
        print(action)

        if action["name"] == "update_firmware":
            update_firmware(action)

        resp = action_complete(action["action_id"])
        print(resp)
        
        send_data(s, resp)

print("Starting Uplink Bridge App")
threading.Thread(target=recv_actions).start()

def send_device_shadow(s, sequence):
    t = int(time.time()*1000)
    payload = {
        "stream": "device_shadow",
        "sequence": sequence,
        "timestamp": t,
        "Status": "running" 
    }

    send_data(s, payload)

sequence = 1
while True:
    time.sleep(5)
    send_device_shadow(s, sequence)
    sequence += 1
