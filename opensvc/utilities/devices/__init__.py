import importlib
import platform

_sysname = platform.uname()[0].lower().replace("-", "")
_package = __package__ or __spec__.name # pylint: disable=undefined-variable
_os = importlib.import_module("." + _sysname, package=_package)
_module_dict = _os.__dict__

def devs_to_disks(self, devs=None):
    return devs or set()

#
# Override generic definitions by os-specific ones
#
try:
    globals().update({"devs_to_disks": _os.devs_to_disks})
except AttributeError:
    pass
try:
    globals().update({"promote_dev_rw": _os.devs_to_disks})
except AttributeError:
    pass

