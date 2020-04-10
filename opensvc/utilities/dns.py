import json
import socket

import core.exceptions as ex
from env import Env
from utilities.string import bencode, bdecode

def zone_list(zone, nameservers):
    for nameserver in nameservers:
        try:
            return _zone_list(zone, nameserver)
        except Exception:
            continue

def _zone_list(zone, nameserver):
    request = {
        "method": "list",
        "parameters": { 
            "zonename": zone,
        }
    }
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(Env.paths.dnsuxsock)
        sock.send(bencode(json.dumps(request)+"\n"))
        response = ""
        while True:
            buff = sock.recv(4096)
            if not buff:
                break
            response += bdecode(buff)
            if response[-1] == "\n":
                break
    finally:
        sock.close()
    if not response:
        return
    try:
        return json.loads(response)["result"]
    except ValueError:
        raise ex.Error("invalid response format")

