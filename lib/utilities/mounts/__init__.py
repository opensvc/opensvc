import importlib
import platform


_sysname = platform.uname()[0].lower().replace("-", "")
_os = importlib.import_module("." + _sysname, package=__package__)
Mounts = _os.Mounts
