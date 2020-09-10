"""
Namespaces functions
"""
import fnmatch
import glob
import importlib
import os
import re
from itertools import chain

import core.exceptions as ex
from core.contexts import want_context
from env import Env


VALID_NAME_RFC952_NO_DOT = (r"^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]))*"
                            r"([A-Za-z]|[A-Za-z][A-Za-z0-9-]*[A-Za-z0-9])$")
VALID_NAME_RFC952 = (r"^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9])\.)*"
                     r"([A-Za-z]|[A-Za-z][A-Za-z0-9-]*[A-Za-z0-9])$")

ANSI_ESCAPE = re.compile(r"\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[mHJKG]", re.UNICODE)
ANSI_ESCAPE_B = re.compile(br"\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[mHJKG]")

def is_service(f, namespace=None, data=None, local=False, kinds=None):
    if f is None:
        return
    f = re.sub(r"\.conf$", '', f)
    f = f.replace(Env.paths.pathetcns + os.sep, "").replace(Env.paths.pathetc + os.sep, "")
    try:
        name, _namespace, kind = split_path(f)
    except ValueError:
        return
    if kinds and kind not in kinds:
        return
    if not namespace:
        namespace = _namespace
    path = fmt_path(name, namespace, kind)
    if not local:
        try:
            data["services"][path]
            return path
        except Exception:
            if want_context():
                return
    cf = svc_pathcf(path)
    if not os.path.exists(cf):
        return
    return path


def list_services(namespace=None, kinds=None):
    l = []
    if namespace in (None, "root"):
        for name in glob_root_config():
            s = name[:-5]
            if len(s) == 0:
                continue
            path = is_service(name, kinds=kinds)
            if path is None:
                continue
            l.append(path)
    n = len(os.path.join(Env.paths.pathetcns, ""))
    for path in glob_ns_config(namespace):
        path = path[n:-5]
        if not path or path[-1] == os.sep:
            continue
        if kinds:
            try:
                name, namespace, kind = split_path(path)
            except ValueError:
                continue
            if kind not in kinds:
                continue
        if path.endswith("/namespace") and path.count("/") == 1:
            path = path[:-9]
        l.append(path)
    return l


def glob_root_config():
    GLOB_ROOT_SVC_CONF = os.path.join(Env.paths.pathetc, "*.conf")
    GLOB_ROOT_VOL_CONF = os.path.join(Env.paths.pathetc, "vol", "*.conf")
    GLOB_ROOT_CFG_CONF = os.path.join(Env.paths.pathetc, "cfg", "*.conf")
    GLOB_ROOT_SEC_CONF = os.path.join(Env.paths.pathetc, "sec", "*.conf")
    GLOB_ROOT_USR_CONF = os.path.join(Env.paths.pathetc, "usr", "*.conf")
    return chain(
        glob.iglob(GLOB_ROOT_SVC_CONF),
        glob.iglob(GLOB_ROOT_VOL_CONF),
        glob.iglob(GLOB_ROOT_CFG_CONF),
        glob.iglob(GLOB_ROOT_SEC_CONF),
        glob.iglob(GLOB_ROOT_USR_CONF),
    )


def glob_ns_config(namespace=None):
    if namespace is None:
        GLOB_CONF_NSCFG_CONF = os.path.join(Env.paths.pathetcns, "*", "namespace.conf")
        GLOB_CONF_NS = os.path.join(Env.paths.pathetcns, "*", "*", "*.conf")
    else:
        GLOB_CONF_NSCFG_CONF = os.path.join(Env.paths.pathetcns, namespace, "namespace.conf")
        GLOB_CONF_NS = os.path.join(Env.paths.pathetcns, namespace, "*", "*.conf")
    return chain(
        glob.iglob(GLOB_CONF_NSCFG_CONF),
        glob.iglob(GLOB_CONF_NS),
    )


def glob_services_config():
    return chain(glob_root_config(), glob_ns_config())


def is_ns_path(path):
    try:
        return path.endswith("/") and path.count("/") == 1
    except ValueError:
        return False

def split_path(path):
    if is_ns_path(path):
        return "namespace", path.strip("/") or None, "nscfg"
    path = path.strip("/")
    if path in ("node", "auth"):
        raise ValueError
    if "," in path or "+" in path:
        raise ValueError
    nsep = path.count("/")
    if nsep == 2:
        namespace, kind, name = path.split("/")
    elif nsep == 1:
        kind, name = path.split("/")
        namespace = "root"
    elif nsep == 0:
        name = path
        namespace = "root"
        kind = "svc"
    else:
        raise ValueError(path)
    if namespace == "root":
        namespace = None
        if name == "cluster":
            kind = "ccfg"
        elif name == "namespace":
            kind = "nscfg"
    return name, namespace, kind


def svc_pathcf(path, namespace=None):
    name, _namespace, kind = split_path(path)
    if namespace:
        if kind == "nscfg":
            return os.path.join(Env.paths.pathetcns, namespace, "namespace.conf")
        return os.path.join(Env.paths.pathetcns, namespace, kind, name + ".conf")
    elif _namespace:
        if kind == "nscfg":
            return os.path.join(Env.paths.pathetcns, _namespace, "namespace.conf")
        return os.path.join(Env.paths.pathetcns, _namespace, kind, name + ".conf")
    elif kind in ("svc", "ccfg"):
        return os.path.join(Env.paths.pathetc, name + ".conf")
    elif kind == "nscfg":
        return os.path.join(Env.paths.pathetc, "namespace.conf")
    else:
        return os.path.join(Env.paths.pathetc, kind, name + ".conf")


def svc_pathetc(path, namespace=None):
    return os.path.dirname(svc_pathcf(path, namespace=namespace))


def svc_pathtmp(path):
    name, namespace, kind = split_path(path)
    if namespace:
        return os.path.join(Env.paths.pathtmp, "namespaces", namespace, kind)
    elif kind in ("svc", "ccfg"):
        return os.path.join(Env.paths.pathtmp)
    else:
        return os.path.join(Env.paths.pathtmp, kind)


def svc_pathlog(path):
    name, namespace, kind = split_path(path)
    if namespace:
        return os.path.join(Env.paths.pathlog, "namespaces", namespace, kind)
    elif kind in ("svc", "ccfg"):
        return os.path.join(Env.paths.pathlog)
    else:
        return os.path.join(Env.paths.pathlog, kind)


def svc_pathvar(path, relpath=""):
    name, namespace, kind = split_path(path)
    if namespace:
        l = [Env.paths.pathvar, "namespaces", namespace, kind, name]
    else:
        l = [Env.paths.pathvar, kind, name]
    if relpath:
        l.append(relpath)
    return os.path.join(*l)


def fmt_path(name, namespace, kind):
    if namespace not in (None, "root"):
        ns = namespace.strip("/")
        if kind == "nscfg":
            return ns + "/"
        return "/".join((ns, kind, name))
    elif kind not in ("svc", "ccfg"):
        if kind == "nscfg":
            return "/"
        return "/".join((kind, name))
    else:
        return name


def split_fullname(fullname, clustername):
    fullname = fullname[:-(len(clustername) + 1)]
    return fullname.rsplit(".", 2)


def svc_fullname(name, namespace, kind, clustername):
    return "%s.%s.%s.%s" % (
        name,
        namespace if namespace else "root",
        kind,
        clustername
    )


def strip_path(paths, namespace):
    if not namespace:
        return paths
    if isinstance(paths, (list, tuple)):
        return [strip_path(path, namespace) for path in paths]
    else:
        path = re.sub("^%s/" % namespace, "", paths)  # strip current ns
        return re.sub("^svc/", "", path)  # strip default kind


def path_data(path):
    name, namespace, kind = split_path(path)
    if namespace is None:
        namespace = "root"
    return {
        "path": path,
        "name": name,
        "namespace": namespace,
        "kind": kind,
        "normalized": "%s/%s/%s" % (namespace, kind, name),
        "display": fmt_path(name, namespace, kind),
    }


def paths_data(paths):
    for path in paths:
        yield path_data(path)


def resolve_path(path, namespace=None):
    """
    Return the path, parented in <namespace> if specified and if not found
    in <path>.
    """
    name, _namespace, kind = split_path(path)
    if namespace and not _namespace:
        _namespace = namespace
    if _namespace == "root":
        _namespace = None
    return fmt_path(name, _namespace, kind)


def validate_paths(paths):
    [validate_path(p) for p in paths]


def validate_path(path):
    name, namespace, kind = split_path(path)
    validate_kind(kind)
    validate_ns_name(namespace)
    validate_name(name)


def validate_kind(name):
    if name not in Env.kinds:
        raise ValueError("invalid kind '%s'. kind must be one of"
                         " %s." % (name, ", ".join(Env.kinds)))


def validate_ns_name(name):
    if name is None:
        return
    if name in Env.kinds:
        raise ValueError("invalid namespace name '%s'. names must not clash with kinds"
                         " %s." % (name, ", ".join(Env.kinds)))
    if re.match(VALID_NAME_RFC952_NO_DOT, name):
        return
    raise ValueError("invalid namespace name '%s'. names must contain only letters, "
                     "digits and hyphens, start with a letter and end with "
                     "a digit or letter (rfc 952)." % name)


def validate_name(name):
    # strip scaler slice prefix
    name = re.sub(r"^[0-9]+\.", "", name)
    if name in Env.kinds:
        raise ex.Error("invalid name '%s'. names must not clash with kinds"
                          " %s." % (name, ", ".join(Env.kinds)))
    if re.match(VALID_NAME_RFC952, name):
        return
    raise ex.Error("invalid name '%s'. names must contain only dots, letters, "
                      "digits and hyphens, start with a letter and end with "
                      "a digit or letter (rfc 952)." % name)


def parse_path_selector(selector, namespace=None):
    if selector is None:
        if namespace:
            return "*", namespace, "svc"
        else:
            return "*", "*", "svc"
    elts = selector.split("/")
    elts_count = len(elts)
    if elts_count == 1:
        if elts[0] == "**":
            _namespace = namespace if namespace else "*"
            _kind = "*"
            _name = "*"
        elif elts[0] == "*":
            _namespace = namespace if namespace else "*"
            _kind = "svc"
            _name = "*"
        else:
            _namespace = namespace if namespace else "*"
            _kind = "svc"
            _name = elts[0]
    elif elts_count == 2:
        if elts[0] == "**":
            _namespace = namespace if namespace else "*"
            _kind = "*"
            _name = elts[1] if elts[1] not in ("**", "") else "*"
        elif elts[0] == "*":
            _namespace = namespace if namespace else "*"
            _kind = "svc"
            _name = elts[1] if elts[1] not in ("**", "") else "*"
        elif elts[1] == "**":
            _namespace = namespace if namespace else elts[0]
            _kind = "*"
            _name = "*"
        elif elts[0] == "*":
            _namespace = namespace if namespace else elts[0]
            _kind = "svc"
            _name = "*"
        else:
            _namespace = "root"
            _kind = elts[0]
            _name = elts[1]
    elif elts_count == 3:
        _namespace = namespace if namespace else elts[0]
        _kind = elts[1]
        _name = elts[2]
    else:
        raise ValueError("invalid path selector %s" % selector)
    return _name, _namespace, _kind


def format_path_selector(selector, namespace=None, maxlen=None):
    try:
        _name, _namespace, _kind = parse_path_selector(selector, namespace)
        buff = "%s/%s/%s" % (_namespace, _kind, _name)
    except ValueError:
        buff = selector
    if maxlen:
        if len(buff) > maxlen:
            buff = buff[:maxlen-3] + "..."
    return buff

def normalize_jsonpath(path):
    if path and path[0] == ".":
        path = path[1:]
    return path


def abbrev(l):
    if len(l) < 1:
        return l
    paths = [n.split(".")[::-1] for n in l]
    trimable = [n for n in paths if len(n) > 1]
    if len(trimable) <= 1:
        return [n[-1] + ".." if n in trimable else n[0] for n in paths]
    for i in range(10):
        try:
            if len(set([t[i] for t in trimable])) > 1:
                break
        except IndexError:
            break
    if i == 0:
        return l
    return [".".join(n[:i - 1:-1]) + ".." if n in trimable else n[0] for n in paths]


def factory(kind):
    """
    Return a Svc or Node object
    """
    if kind == "node":
        from core.node import Node
        return Node
    try:
        mod = importlib.import_module("core.objects."+kind)
        return getattr(mod, kind.capitalize())
    except Exception as exc:
        print(exc)
        pass
    raise ValueError("unknown kind: %s" % kind)


def new_id():
    import uuid
    return str(uuid.uuid4())

def object_path_glob(pattern, pds=None, namespace=None, kind=None, negate=False):
    pds = pds or []
    if kind:
        pds = [pd for pd in pds if pd["kind"] == kind]
    l = pattern.split("/")
    n = len(l)
    if n == 3:
        if l[1] == "nscfg":
            # */nscfg/*
            _selector = "%s/nscfg/namespace" % l[0]
        elif not l[2]:
            # test/svc/
            _selector = "%s/%s/*" % (l[0], l[1])
        else:
            # a*/b*/c*
            _selector = pattern
    elif n == 2:
        if not l[1]:
            # pg1/
            _selector = "%s/nscfg/namespace" % l[0]
        elif l[1] == "**":
            # prod/**
            _selector = "%s/*/*" % l[0]
        elif l[0] == "**":
            # **/s*
            _selector = "*/*/%s" % l[1]
        else:
            # svc/s*
            _selector = "%s/%s/%s" % (namespace or "root", l[0], l[1])
    elif n == 1:
        if l[0] == "**":
            _selector = "*/*/*"
        else:
            _selector = "%s/%s/%s" % (namespace or "root", kind or "svc", l[0])
    else:
        return []
    return [pd["display"] for pd in pds if negate ^ fnmatch.fnmatch(pd["normalized"], _selector)]


