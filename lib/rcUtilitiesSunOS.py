from rcUtilities import justcall

def check_ping(addr, timeout=5, count=1):
    cmd = ['ping', addr, "%s" % timeout]
    out, err, ret = justcall(cmd)
    if ret == 0:
        return True
    return False

def get_os_ver():
    cmd = ['uname', '-v']
    out, err, ret = justcall(cmd)
    if ret != 0:
        return self.undef
    lines = out.split('\n')
    if len(lines) == 0:
        return self.undef
    try:
        osver = float(lines[0])
    except:
        osver = 0
    return osver
