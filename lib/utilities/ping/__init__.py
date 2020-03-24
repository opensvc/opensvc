import importlib
import platform

sysname = platform.uname()[0].lower().replace("-", "")
mod = importlib.import_module("." + sysname, package=__package__)
check_ping = mod.check_ping
