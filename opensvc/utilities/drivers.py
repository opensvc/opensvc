import importlib
import pkgutil

from env import Env


def driver_import(*args, **kwargs):
    try:
        return _driver_import(*args, **kwargs)
    except ImportError as exc:
        kwargs["head"] = "site-opensvc.drivers"
        return _driver_import(*args, **kwargs)

def _driver_import(*args, head="drivers", fallback=True, initial_modname=None):
    def fmt_element(s):
        if s is None:
            return ""
        # Linux => linux
        # SunOS => sunos
        # HP-UX => hpux
        return s.lower().replace("-", "")

    def fmt_modname(args):
        l = [head]
        for i, e in enumerate(args):
            if e == "":
                continue
            if i == 0:
                if e == "res":
                    e = "resource"
                l.append(e)
            else:
                l.append(fmt_element(e))
        return ".".join(l).rstrip(".")

    def import_mod(modname):
        for mn in (modname + "." + fmt_element(Env.sysname), modname):
            try:
                m = importlib.import_module(mn)
                return m
            except ImportError as exc:
                pass

    modname = fmt_modname(args)
    if not initial_modname:
        initial_modname = modname

    mod = import_mod(modname)
    if mod:
        if args[0] == "resource" and not hasattr(mod, "adder"):
            raise ImportError("no module found: %s" % initial_modname)
        return mod
    if fallback and len(args) > 2:
        args = args[:-1]
        return _driver_import(*args, fallback=fallback, initial_modname=initial_modname)
    else:
        raise ImportError("no module found: %s" % initial_modname)


def iter_drivers(groups=None):
    groups = groups or [""]
    for group in groups:
        try:
            package = importlib.import_module("drivers.resource."+group)
        except ImportError:
            continue
        for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
            if not ispkg:
                continue
            yield driver_import("resource", group, modname)

