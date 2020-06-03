from subprocess import *

from utilities.proc import which

def change_root_pw(pw):
    if which('pw') is None:
        print("pw command not found")
        return 1
    cmd = ['pw', 'user', 'mod', 'root', '-h', '0']
    _input = pw
    p = Popen(cmd, stdin=PIPE)
    p.stdin.write(_input)
    p.communicate()
    return p.returncode
