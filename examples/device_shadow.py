import socket
import json
import time
import os
import stat
import shutil
import threading

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 5555))

# Constructs a payload and sends it over TCP to uplink
def send_data(s, payload):
    send = json.dumps(payload) + "\n"
    s.sendall(bytes(send, encoding="utf-8"))


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
