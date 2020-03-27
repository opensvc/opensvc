import importlib
import platform

_sysname = platform.uname()[0].lower().replace("-", "")
_os = importlib.import_module("." + _sysname, package=__package__)
_module_dict = _os.__dict__

class DevTree(object):
    pass

#
# Override generic definitions by os-specific ones
#
try:
    globals().update({"DevTree": _os.DevTree})
except AttributeError:
    pass

