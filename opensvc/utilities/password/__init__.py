import importlib
import platform

_sysname = platform.uname()[0].lower().replace("-", "")
_package = __package__ or __spec__.name
_os = importlib.import_module("." + _sysname, package=_package)
change_root_pw = _os.change_root_pw