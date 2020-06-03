from utilities.proc import justcall

def check_ping(addr, timeout=5, count=1):
    cmd = ['ping', '-c', str(count), '-w', str(timeout)]
    if ':' in addr:
        cmd += ['-a', 'inet6']
    cmd += [addr]
    _, _, ret = justcall(cmd)
    if ret == 0:
        return True
    return False
