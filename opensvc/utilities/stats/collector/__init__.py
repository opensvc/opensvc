import importlib
from env import Env

_package = __package__ or __spec__.name # pylint: disable=undefined-variable
_os = importlib.import_module("." + Env.module_sysname, package=_package)
collect = _os.collect
