import importlib
import platform

_sysname = platform.uname()[0].lower().replace("-", "")
_os = importlib.import_module("." + _sysname, package=__package__)
_module_dict = _os.__dict__
Asset = _os.Asset

