try:
    from foreign.six.moves import winreg
except ImportError:
    pass

def get_registry_value(key, subkey, value):
    key = getattr(winreg, key)
    handle = winreg.OpenKey(key, subkey)
    value, type = winreg.QueryValueEx(handle, value)
    return value

