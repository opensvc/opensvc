import os
import re
import json

from rcGlobalEnv import *
from rcUtilities import justcall, which, cache
import rcStatus
import rcExceptions as ex

def lofiadm_data():
    data = {}
    cmd = ["lofiadm"]
    output, err, ret = justcall(cmd)
    for line in output.split('\n'):
        if line.startswith('Block Device'):
            continue
        if line.strip():
            fields = line.strip().split()
            key = fields[0]
            val = fields[1]
            data[key] = val
    return data

def file_to_loop(f):
    """
    Given a file path, returns the loop device associated. For example,
    /path/to/file => /dev/lofi/2
    """
    data = lofiadm_data()
    for key in data.keys():
        if f == data[key]:
            return key
    return []

def loop_to_file(l):
    """
    Given a loop dev, returns the loop file associated. For example,
    /dev/loop0 => /path/to/file
    """
    data = lofiadm_data()
    if data[l]:
        return data[l]
    return []
