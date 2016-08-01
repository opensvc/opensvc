import os
import re

from rcGlobalEnv import *
from rcUtilities import call, which
import rcStatus
import resLoop as Res
import rcExceptions as ex

def file_to_loop(f):
    """Given a file path, returns the loop device associated. For example,
    /path/to/file => /dev/loop0
    """
    if which('losetup') is None:
        return []
    if not os.path.isfile(f):
        return []
    if rcEnv.sysname != 'Linux':
        return []
    (ret, out, err) = call(['losetup', '-j', f])
    if len(out) == 0:
        return []
    """ It's possible multiple loopdev are associated with the same file
    """
    devs= []
    for line in out.split('\n'):
        l = line.split(':')
        if len(l) == 0:
            continue
        if len(l[0]) == 0:
            continue
        if not os.path.exists(l[0]):
            continue
        devs.append(l[0])
    return devs

