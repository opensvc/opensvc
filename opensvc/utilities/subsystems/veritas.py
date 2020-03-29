import os

from utilities.proc import justcall

def vx_dev_to_paths(dev):
    dev = os.path.basename(dev)
    cmd = ["vxdmpadm", "list", "dmpnode", "dmpnodename="+dev]
    out, err, ret = justcall(cmd)
    data = []
    for line in out.splitlines():
        if not line.startswith("path"):
            continue
        path = "/dev/"+line.split("= ", 1)[1].split()[0]
        data.append(path)
    return data
