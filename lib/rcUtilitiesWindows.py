import os
import re
from rcUtilities import justcall
import string
from ctypes import windll


def check_ping(addr, timeout=5, count=1):
    ping = 'ping.exe'
    cmd = [ping,
           '-n', repr(count),
	   '-w', repr(timeout),
	   addr]
    out, err, ret = justcall(cmd)
    if ret == 0:
        return True
    return False

def get_registry_value(key, subkey, value):
    import _winreg
    key = getattr(_winreg, key)
    handle = _winreg.OpenKey(key, subkey)
    (value, type) = _winreg.QueryValueEx(handle, value)
    return value

def get_drives():
    drives = []
    bitmask = windll.kernel32.GetLogicalDrives()
    for letter in string.uppercase:
        if bitmask & 1:
            drives.append(letter)
        bitmask >>= 1
    return drives
