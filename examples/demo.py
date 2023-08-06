import socket
import json
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5050))

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
        "action_id": id,
        "state": "Completed",
        "progress": 100,
        "errors": []
    }

def update_config(action):
    payload = json.loads(action['payload'])
    print(payload)
    app = payload["name"]
    print(app)
    ver = payload["version"]
    print(ver)
    if(ver == "latest"):
        cmd = "sudo apt update && sudo apt install " + app 
        print(cmd)
    resp = action_complete(action["action_id"])
    print(resp)
    send_data(s, resp)


print("Starting Uplink Bridge App")
while True:
	action = recv_action(s)
	print(action)
        
	if action["name"] == "update_config":
		print("update_config action received")
		print(json.loads(action['payload']))
		update_config(action)
	else:
		print(f"Received unknown action type: {action}")
    
