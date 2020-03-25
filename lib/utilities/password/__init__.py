import importlib
import platform

_sysname = platform.uname()[0].lower().replace("-", "")
_os = importlib.import_module("." + _sysname, package=__package__)
change_root_pw = _os.change_root_pw
