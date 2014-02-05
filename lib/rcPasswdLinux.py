from subprocess import *

def change_root_pw(pw):
    cmd = ['passwd', '--stdin', 'root']
    p = Popen(cmd, stdin=PIPE)
    p.stdin.write(pw)
    p.communicate()
    return p.returncode
