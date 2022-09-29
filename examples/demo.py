import socket
import json
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5555))

# Converts JSON data received over TCP into a python dictionary
def recv_action(s):
    return json.loads(s.recv(2048))

# Constructs a payload and sends it over TCP to uplink
def send_data(s, payload):
    t = int(time.time()*1000000)
    send = json.dumps({
        "stream": "action_status",
        "sequence": 0,
        "timestamp": t,
        "payload": payload
    })
    s.sendall(bytes(send, encoding="utf-8"))

# Constructs a JSON `action_status` as a response to received action on completion
def action_complete(id):
    return {
        "id": id,
        "state": "Completed",
        "progress": 100,
        "errors": []
    }

while True:
    action = recv_action(s)
    resp = action_complete(action["action_id"])
    time.sleep(5)
    send_data(s, resp)
