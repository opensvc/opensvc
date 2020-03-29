from utilities.proc import justcall

def check_ping(addr, timeout=5, count=1):
    if ':' in addr:
        ping = 'ping6'
    else:
        ping = 'ping'
    cmd = [ping, '-c', str(count),
                 '-W', str(timeout),
                 '-t', str(timeout),
                 addr]
    _, _, ret = justcall(cmd)
    if ret == 0:
        return True
    return False


