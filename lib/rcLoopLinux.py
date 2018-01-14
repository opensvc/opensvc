import os
import re

from rcGlobalEnv import *
from rcUtilities import justcall, which
import rcStatus
import rcExceptions as ex

def file_to_loop(f):
    """
    Given a file path, returns the loop device associated. For example,
    /path/to/file => /dev/loop0
    """
    if which(rcEnv.syspaths.losetup) is None:
        return []
    if not os.path.isfile(f):
        return []
    if rcEnv.sysname != 'Linux':
        return []
    out, err, ret = justcall([rcEnv.syspaths.losetup, '-j', f])
    if len(out) == 0:
        return []

    # It's possible multiple loopdev are associated with the same file
    devs = []
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

def loop_to_file(f):
    """
    Given a loop dev, returns the loop file associated. For example,
    /dev/loop0 => /path/to/file
    """
    if which(rcEnv.syspaths.losetup) is None:
        return []
    if not os.path.exists(f):
        return []
    if rcEnv.sysname != 'Linux':
        return []
    out, err, ret = justcall([rcEnv.syspaths.losetup, f])
    if len(out) == 0:
        return []

    for line in out.split('\n'):
        l = line.split('(')
        if len(l) == 0:
            continue
        fpath = l[-1].rstrip(")")
        if len(fpath) == 0:
            continue
        if not os.path.exists(fpath):
            continue
        return fpath

