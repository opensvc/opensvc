import importlib
import pkgutil

from rcGlobalEnv import rcEnv


def driver_import(*args, **kwargs):
    def fmt_element(s):
        if s is None:
            return ""
        # Linux => linux
        # SunOS => sunos
        # HP-UX => hpux
        return s.lower().replace("-", "")

    def fmt_modname(args):
        l = ["drivers"]
        for i, e in enumerate(args):
            if e == "":
                continue
            if i == 0:
                if e == "res":
                    e = "resource"
                l.append(e)
            else:
                l.append(fmt_element(e))
        return ".".join(l)

    def import_mod(modname):
        for mn in (modname + "." + fmt_element(rcEnv.sysname), modname):
            try:
                m = importlib.import_module(mn)
                return m
            except ImportError:
                pass

    modname = fmt_modname(args)
    mod = import_mod(modname)

    if mod:
        return mod
    if not kwargs.get("head"):
        kwargs["head"] = modname
    if kwargs.get("fallback", True) and len(args) > 2:
        args = args[:-1]
        return driver_import(*args, **kwargs)
    else:
        raise ImportError("no module found: %s" % kwargs["head"])


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

