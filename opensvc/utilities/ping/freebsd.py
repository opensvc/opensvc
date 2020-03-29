from utilities.proc import justcall

def check_ping(addr, timeout=5, count=1):
    if ':' in addr:
        cmd = ['ping6']
    else:
        cmd = ['ping', '-W', str(timeout)]
    cmd += ['-c', repr(count), addr]
    _, _, ret = justcall(cmd)
    if ret == 0:
        return True
    return False
