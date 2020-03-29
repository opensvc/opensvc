import importlib
import platform

_sysname = platform.uname()[0].lower().replace("-", "")
_package = __package__ or __spec__.name
_mod = importlib.import_module("." + _sysname, package=_package)
check_ping = _mod.check_ping
