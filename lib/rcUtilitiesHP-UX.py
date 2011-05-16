from rcUtilities import call

def check_ping(addr, timeout=5, count=1):
    cmd = ['ping', addr,
                   '-n', repr(count),
                   '-m', repr(timeout)]
    (ret, out, err) = call(cmd)
    if ret == 0:
        return True
    return False

