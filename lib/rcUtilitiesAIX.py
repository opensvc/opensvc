import os
import re
from rcUtilities import call

def check_ping(addr, timeout=5, count=1):
    cmd = ['ping', '-c', repr(count),
                   '-w', repr(timeout)]
    if ':' in addr:
        cmd += ['-a', 'inet6']
    cmd += [addr]
    (ret, out, err) = call(cmd)
    if ret == 0:
        return True
    return False

