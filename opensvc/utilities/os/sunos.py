from utilities.proc import justcall
def get_solaris_version():
    """
    Solaris 2.6 is SunOS 5.6 : osver = 6.0
    Solaris 7 is SunOS 5.7   : osver = 7.0
    Solaris 8 is SunOS 5.8   : osver = 8.0
    ...
    Solaris 11 is SunOS 5.11 : osver = 11.0
    Solaris 11U1 is SunOS 5.11 : osver = 11.1
    Solaris 11U2 is SunOS 5.11 : osver = 11.2
    """
    cmd = ['uname', '-r']
    out, err, ret = justcall(cmd)
    if ret != 0:
        return 0
    lines = out.split('\n')
    if len(lines) == 0:
        return 0
    try:
        base, osver = lines[0].split('.')
        osver = int(osver)
    except ValueError:
        osver = 0

    if osver >= 11:
        cmd = ['uname', '-v']
        out, err, ret = justcall(cmd)
        if ret == 0:
            elts = out.split("\n")[0].split(".")[:2]
            osver = ".".join(elts)

    return float(osver)

