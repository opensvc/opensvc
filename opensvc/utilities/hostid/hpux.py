from subprocess import Popen, PIPE

from utilities.proc import which

def hostid():
    if which('getconf') is None:
        return '1'
    cmd = ['getconf', 'MACHINE_SERIAL']
    p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
    buff = p.communicate()
    sn = buff[0].split()[0]
    if p.returncode != 0:
        return '1'
    return str(hex(abs(sn.__hash__()))).replace('0x', '')
