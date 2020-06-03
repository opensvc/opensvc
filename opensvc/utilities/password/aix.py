from subprocess import *

from utilities.proc import which

def change_root_pw(pw):
    if which('chpasswd') is not None:
        cmd = ['chpasswd']
        _input = "root:" + pw
    else:
        cmd = ['passwd', '-stdin', 'root']
        _input = pw
    p = Popen(cmd, stdin=PIPE)
    p.stdin.write(_input)
    p.communicate()
    return p.returncode
