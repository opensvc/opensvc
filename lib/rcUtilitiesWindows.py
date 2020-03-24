import string
from ctypes import windll

try:
    from six.moves import winreg
except ImportError:
    pass

def get_registry_value(key, subkey, value):
    key = getattr(winreg, key)
    handle = winreg.OpenKey(key, subkey)
    value, type = winreg.QueryValueEx(handle, value)
    return value

def get_drives():
    drives = []
    bitmask = windll.kernel32.GetLogicalDrives()
    for letter in string.ascii_uppercase:
        if bitmask & 1:
            drives.append(letter)
        bitmask >>= 1
    return drives

def devs_to_disks(self, devs=set()):
    return devs
