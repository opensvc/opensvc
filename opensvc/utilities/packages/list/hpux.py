import datetime

from env import Env
from utilities.proc import call, which


def listpkg():
    if which('swlist') is None:
        return []
    lines = []
    for t in ('product', 'bundle'):
        lines += listpkg_t(t)
    return lines

def listpkg_t(t):
    cmd = ['swlist', '-l', t, '-a', 'revision', '-a', 'mod_time']
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 3:
            continue
        if line[0] == '#':
            continue
        try:
            l[2] = datetime.datetime.fromtimestamp(int(l[2])).strftime("%Y-%m-%d %H:%M:%S")
        except:
            l[2] = ""
        x = [Env.nodename, l[0], l[1], '', t, l[2]]
        lines.append(x)
    return lines

def listpatch():
    return []
