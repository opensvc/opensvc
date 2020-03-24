import importlib
import platform

_sysname = platform.uname()[0].lower().replace("-", "")
_os = importlib.import_module("." + _sysname, package=__package__)
_module_dict = _os.__dict__

def devs_to_disks(self, devs=set()):
    return devs

#
# Override generic definitions by os-specific ones
#
try:
    globals().update({"devs_to_disks": _os.devs_to_disks})
except AttributeError:
    pass

