import socket
import json
import time
import os
import stat
import shutil
import threading
import zipfile
import subprocess

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

def action_failed(id, reason):
    return {
        "stream": "action_status",
        "sequence": 0,
        "timestamp": int(time.time()*1000000),
        "id": id,
        "state": "Failed",
        "progress": 100,
        "errors": [reason]
    }


def update_firmware(action_id, payload_json):
    payload = json.loads(payload_json)
    print("payload: ")
    print(payload)

    zip_file_path = payload['download_path']
    print("zip_file_path: ")
    print(zip_file_path)

    dest_path = '/mnt/download/'
    #dest_path = zip_file_path
    
    """
    if os.path.exists(dest_path):
        print("Deleting destination path")
        shutil.rmtree(dest_path)
    for f in os.listdir(dest_path):
        os.remove(os.path.join(dest_path, f))
    """
    print("zip file extraction")
    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        print("Inside zip file extraction logic")
        #root_path, = zipfile.Path(zip_file).iterdir()

        root_path_new = zipfile.Path(zip_file)
        
        print("Extracting files to %s"%dest_path)
        zip_file.extractall(dest_path)

        script_cwd = "/mnt/download/"
        script_path = "/mnt/download/update.sh"

        if os.path.exists(script_path):
            os.chmod(script_path, 0o755)
            print("Running update.sh script")
            subprocess.run(script_path, cwd=script_cwd)
        else:
            print("Path %s does not exist"%script_path)
            return action_failed(action_id, "Could not find the update script")
        
        # run /mnt/download/update_fstab_next_root.sh
        if os.path.exists("/mnt/download/update_fstab_next_root.sh"):
            os.chmod("/mnt/download/update_fstab_next_root.sh", 0o755)
            print("Running script")
            subprocess.run("/mnt/download/update_fstab_next_root.sh", cwd="/mnt/download")
        else:
            print("Path %s does not exist"%"/mnt/download/update_fstab_next_root.sh")
            return action_failed(action_id, "Could not find the script, update_fstab_next_root.sh")

        # run /mnt/download/systemd/systemd.sh
        if os.path.exists("/mnt/download/systemd/systemd.sh"):
            os.chmod("/mnt/download/systemd/systemd.sh", 0o755)
            print("Running script")
            subprocess.run("/mnt/download/systemd/systemd.sh", cwd="/mnt/download/systemd")
        else:
            print("Path %s does not exist"%"/mnt/download/systemd/systemd.sh")
            return action_failed(action_id, "Could not find the script, systemd.sh")

        # run /mnt/download/reboot.sh
        if os.path.exists("/mnt/download/reboot.sh"):
            os.chmod("/mnt/download/reboot.sh", 0o755)
            print("Running script")
            subprocess.run("/mnt/download/reboot.sh", cwd="/mnt/download/")
        else:
            print("Path %s does not exist"%"/mnt/download/reboot.sh")
            return action_failed(action_id, "Could not find the script, reboot.sh")
    return action_complete(action_id)

def recv_actions():
    while True:
        action = recv_action(s)
        print("Received action %s"%str(action))

        action_name = action["name"]
        action_id = action["action_id"]
        action_payload = action["payload"]

        resp = action_failed(action_id, "Action {name} does not exist".format(name=action_name))

        try:
            if action_name == "update_firmware":
                resp = update_firmware(action_id, action_payload)
            elif action_name == "send_file":
                resp = action_complete(action_id)
        except Exception as e: 
            print(e)
            resp = action_failed(action_id, "Failed with exception: {msg}".format(msg=str(e)))

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
