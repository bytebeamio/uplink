import socket
import json
import time

# Create a socket and connect to bridge
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5555))

# Creates and sends template status, with provided state and progress
def reply(s, recv, state, progress):
    # Wait for 5s
    time.sleep(2)

    # Create Payload to contain Action Status
    p = {
        "stream": "action_status",
        "sequence": 0,
        "timestamp": t,
        "payload": {
            "action_id": recv["action_id"],
            "timestamp": t,
            "state": state,
            "progress": progress,
            "errors": []
        }
    }
    print("Replying:\n", p)
    # Send data to uplink
    s.sendall(bytes(json.dumps(p)+"\n", encoding="utf-8"))


while True:
    # Receive data from uplink
    r = s.recv(2048)
    if not r:
        break

    # Decode received json
    recv = json.loads(r)
    print("Received:\n", recv)
    t = int(time.time()*1000000)

    # Status: started execution
    reply(s, recv, "running", 0)
    # Status: completed execution
    reply(s, recv, "completed", 100)
