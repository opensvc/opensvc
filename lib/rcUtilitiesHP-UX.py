from rcUtilities import call

def check_ping(addr, timeout=5, count=1):
    cmd = ['ping', '-n', repr(count),
                   '-m', repr(timeout),
                   '-i', addr]
    (ret, out) = call(cmd)
    if ret == 0:
        return True
    return False

