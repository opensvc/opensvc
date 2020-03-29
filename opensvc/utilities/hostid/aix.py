from subprocess import Popen, PIPE

def hostid():
    cmd = ['uname', '-u']
    p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
    buff = p.communicate()
    if p.returncode != 0:
        return '1'
    sn = buff[0].split()[0]
    return str(hex(abs(sn.__hash__()))).replace('0x', '')
