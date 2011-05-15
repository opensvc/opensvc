from rcUtilities import call

def check_ping(addr, timeout=5, count=1):
    cmd = ['ping', addr, "%s" % timeout]
    (ret, out, err) = call(cmd)
    if ret == 0:
        return True
    return False

