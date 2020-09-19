import importlib
import pkgutil

from env import Env

DEFAULT_HEAD = "drivers"
SITE_HEAD = "site_opensvc.drivers"


def driver_import(*args, **kwargs):
    fallback = kwargs.get('fallback', True)
    head = kwargs.get('head', DEFAULT_HEAD)
    try:
        return _driver_import(*args, head=head)
    except ImportError:
        pass

    initial_modname = "drivers." + ".".join(args)

    if head != SITE_HEAD:
        try:
            return _driver_import(*args, head=SITE_HEAD, initial_modname=initial_modname)
        except ImportError:
            pass

    if not fallback or len(args) <= 2:
        raise ImportError

    args = args[:-1]
    return driver_import(*args, head=head)


def _driver_import(*args, **kwargs):
    head = kwargs.get('head', DEFAULT_HEAD)
    initial_modname = kwargs.get('head')
    def fmt_element(s):
        if s is None:
            return ""
        # Linux => linux
        # SunOS => sunos
        # HP-UX => hpux
        return s.lower().replace("-", "")

    def fmt_modname(elements):
        l = [head]
        for i, e in enumerate(elements):
            if e == "":
                continue
            if i == 0:
                if e == "res":
                    e = "resource"
                l.append(e)
            else:
                l.append(fmt_element(e))
        return ".".join(l).rstrip(".")

    def import_mod(module_name):
        for mn in (module_name + "." + fmt_element(Env.sysname), module_name):
            try:
                m = importlib.import_module(mn)
                return m
            except ImportError:
                pass

    modname = fmt_modname(args)
    initial_modname = initial_modname or modname

    mod = import_mod(modname)
    if mod:
        if not hasattr(mod, "DRIVER_BASENAME") and "drivers.resource." in mod.__name__:
            raise ImportError("module %s does not set DRIVER_BASENAME" % mod.__file__)
        return mod
    raise ImportError("no module found: %s" % initial_modname)


def driver_class(mod):
    """
    Inspect the module and format the expected resource class name
    based on the DRIVER_GROUP and DRIVER_BASENAME attributes.

    For example:
    mod.DRIVER_GROUP = "fs"
    mod.DRIVER_BASENAME = "xfs_two"

    formats and return the "FsXfsTwo" classname.
    """
    def pascalize(s):
        l = [e.capitalize() for e in s.split("_")]
        return "".join(l)

    try:
        classname = pascalize(mod.DRIVER_GROUP)
    except AttributeError:
        return
    try:
        classname += pascalize(mod.DRIVER_BASENAME)
    except AttributeError:
        pass
    return getattr(mod, classname)


def iter_drivers(groups=None):
    groups = groups or []
    for group in groups:
        try:
            package = importlib.import_module("drivers.resource."+group)
        except ImportError:
            continue
        for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
            if not ispkg:
                continue
            try:
                yield driver_import("resource", group, modname)
            except ImportError:
                continue


def load_drivers(groups=None):
    for mod in iter_drivers(groups):
        pass
