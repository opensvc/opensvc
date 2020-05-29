from utilities.proc import justcall

def check_ping(addr, timeout=5, count=1):
    cmd = ['ping.exe',
           '-n', str(count),
           '-w', str(timeout),
           addr]
    _, _, ret = justcall(cmd)
    if ret == 0:
        return True
    return False
