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

# Solaris 2.6 is SunOS 5.6 : osver = 6.0
# Solaris 7 is SunOS 5.7   : osver = 7.0
# Solaris 8 is SunOS 5.8   : osver = 8.0
# ...
# Solaris 11 is SunOS 5.11 : osver = 11.0
# Solaris 11U1 is SunOS 5.11 : osver = 11.1
# Solaris 11U2 is SunOS 5.11 : osver = 11.2
def get_solaris_version():
    cmd = ['uname', '-r']
    out, err, ret = justcall(cmd)
    if ret != 0:
        return self.undef
    lines = out.split('\n')
    if len(lines) == 0:
        return self.undef
    try:
        base, osver = lines[0].split('.')
        osver = int(osver)
    except:
        osver = 0

    if osver >= '11':
        cmd = ['uname', '-v']
        out, err, ret = justcall(cmd)
        if ret == 0:
            lines = out.split('\n')
            osver = lines[0]

    return float(osver)
