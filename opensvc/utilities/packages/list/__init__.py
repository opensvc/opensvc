import importlib
import platform

_sysname = platform.uname()[0].lower().replace("-", "")
_package = __package__ or __spec__.name # pylint: disable=undefined-variable
_os = importlib.import_module("." + _sysname, package=_package)
listpkg = _os.listpkg
listpatch = _os.listpatch

