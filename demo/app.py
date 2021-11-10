from concurrent.futures import ThreadPoolExecutor
import socket
import threading
import json
import time

# Create a socket and connect to bridge
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5555))


def action_response(action_id, state, progress):
    """Generate ActionResponse payload provided executing action information"""
    t = int(time.time()*1000000)
    return payload("action_status", 0, {
        "id": action_id,
        "state": state,
        "timestamp": t,
        "progress": progress,
        "errors": []
    })


def payload(stream, seq, data):
    """Generate payload provided stream and data values"""
    return {
        "stream": stream,
        "sequence": 0,
        "timestamp": int(time.time()*1000000),
        "payload": data
    }


def reply(s, resp):
    """Creates and sends data/action status"""
    # Wait for 5s
    time.sleep(5)

    print("Replying:\n", resp)
    # Send data to uplink
    s.sendall(bytes(json.dumps(resp)+"\n", encoding="utf-8"))


i = 0.0
while i < 10:
    i += 0.1
    gps_data = {"latitude": i + i*10, "longitude": 100 - i + i*0.1}
    payload
    reply(s, payload("gps", i*10, gps_data))

# Starts only after 100 gps data points are sent
while True:
    # Receive data from uplink
    try:
        # Decode received json
        r = s.recv(2048)
        recv = json.loads(r)
        print("Received:\n", recv)
        action_id = recv["action_id"]
        # Status: started execution
        status = action_response(action_id, "Running", 0)
        reply(s, 0, status)
        # Status: completed execution
        status = action_response(action_id, "Completed", 100)
        reply(s, 0, status)
    except:
        break
