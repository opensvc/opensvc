from __future__ import print_function
import os
import sys
import re
import glob
import logging

from rcGlobalEnv import rcEnv
from storage import Storage
import rcLogger
import rcExceptions as ex
import rcConfigParser
from rcUtilities import mimport, list_services, \
                        svc_pathetc, split_path, makedirs, factory

def get_tags(svc, section):
    try:
        s = svc.oget(section, "tags")
    except ValueError as exc:
        s = set()
    return s

def get_optional(svc, section):
    if "noaction" in get_tags(svc, section):
        return True
    return svc.oget(section, "optional")

def get_monitor(svc, section, impersonate=None):
    return svc.oget(section, "monitor", impersonate=impersonate)

def get_provision(svc, section):
    return svc.oget(section, "provision")

def get_unprovision(svc, section):
    return svc.oget(section, "unprovision")

def get_encap(svc, section):
    try:
        return svc.oget(section, "encap")
    except ValueError as exc:
        return False

def get_shared(svc, section):
    return svc.oget(section, "shared")

def get_rcmd(svc, section):
    return svc.oget(section, "rcmd")

def get_subset(svc, section, impersonate=None):
    return svc.oget(section, "subset", impersonate=impersonate)

def get_osvc_root_path(svc, section):
    return svc.oget(section, "osvc_root_path")

def get_restart(svc, section, impersonate=None):
    return svc.oget(section, "restart", impersonate=impersonate)

def get_disabled(svc, section, impersonate=None):
    return svc.oget(section, "disable", impersonate=impersonate)

def get_promote_rw(svc, section):
    try:
        return svc.oget(section, "promote_rw")
    except ValueError as exc:
        return False

def get_standby(svc, section):
    try:
        return svc.conf_get(section, "standby")
    except ex.OptNotFound as exc:
        # backward compat with always_on
        try:
            return standby_from_always_on(svc, section)
        except ex.OptNotFound:
            pass
        return exc.default

def base_kwargs(svc, s):
    """
    Common kwargs for all drivers.
    """
    return {
        "rid": s,
        "subset": get_subset(svc, s),
        "tags": get_tags(svc, s),
        "disabled": get_disabled(svc, s),
        "optional": get_optional(svc, s),
    }

def init_kwargs(svc, s):
    return {
        "rid": s,
        "subset": get_subset(svc, s),
        "tags": get_tags(svc, s),
        "standby": get_standby(svc, s),
        "disabled": get_disabled(svc, s),
        "optional": get_optional(svc, s),
        "monitor": get_monitor(svc, s),
        "skip_provision": not get_provision(svc, s),
        "skip_unprovision": not get_unprovision(svc, s),
        "restart": get_restart(svc, s),
        "shared": get_shared(svc, s),
        "encap": get_encap(svc, s),
        "promote_rw": get_promote_rw(svc, s),
    }

def sync_kwargs(svc, s):
    """
    Common kwargs for all sync drivers.
    """
    kwargs = {}
    kwargs.update(base_kwargs(svc, s))
    kwargs["sync_max_delay"] = svc.oget(s, "sync_max_delay")
    kwargs["schedule"] = svc.oget(s, "schedule")
    return kwargs

def container_kwargs(svc, s, default_name="name"):
    """
    Common kwargs for all container drivers.
    """
    kwargs = {}
    kwargs["osvc_root_path"] = get_osvc_root_path(svc, s)

    try:
        kwargs["name"] = svc.conf_get(s, "name")
    except ex.OptNotFound as exc:
        if default_name is None:
            kwargs["name"] = exc.default
        else:
            kwargs["name"] = svc.name

    kwargs["guestos"] = svc.oget(s, "guestos")
    kwargs["start_timeout"] = svc.oget(s, "start_timeout")
    kwargs["stop_timeout"] = svc.oget(s, "stop_timeout")
    return kwargs

def standby_from_always_on(svc, section):
    always_on_opt = svc.conf_get(section, "always_on")
    if rcEnv.nodename in always_on_opt:
        return True
    if "nodes" in always_on_opt and rcEnv.nodename in svc.nodes:
        return True
    if "drpnodes" in always_on_opt and rcEnv.nodename in svc.drpnodes:
        return True
    return False

def add_resource(svc, driver_group, s):
    if driver_group == "pool":
        driver_group = "zpool"
        match = "[z]{0,1}pool#"
    else:
        match = driver_group+"#"

    if driver_group in ("disk", "vg", "zpool") and re.match(match+".+pr$", s, re.I) is not None:
        # persistent reserv resource are declared by their peer resource:
        # don't add them from here
        return

    try:
        driver_basename = svc.oget(s, "type")
    except Exception:
        driver_basename = ""

    try:
        mod = svc.load_driver(driver_group, driver_basename)
    except Exception:
        return

    tags = get_tags(svc, s)
    encap = get_encap(svc, s)

    if svc.encap and "encap" not in tags and not encap:
        return

    if not svc.encap and (encap or "encap" in tags):
        svc.has_encap_resources = True
        try:
            enode = list(svc.encapnodes)[0]
        except IndexError:
            return
        svc.encap_resources[s] = Storage({
            "rid": s,
            "tags": tags,
            "encap": encap,
            "subset": svc.oget(s, "subset", impersonate=enode),
            "nb_restart": get_restart(svc, s, impersonate=enode),
            "monitor": get_monitor(svc, s, impersonate=enode),
            "disabled": get_disabled(svc, s, impersonate=enode),
        })
        svc.encap_resources[s].is_disabled = lambda: svc.encap_resources[s].disabled
        return

    if s in svc.resources_by_id:
        return

    if not hasattr(mod, "adder"):
        raise ImportError("%s returned by load_driver(%s, %s) does not have a adder" % (mod, driver_group, driver_basename))

    mod.adder(svc, s)

def add_mandatory_syncs(svc):
    """
    Mandatory files to sync.
    To all nodes, service definition and files contributed by resources.
    """

    def add_file(flist, fpath):
        if not os.path.exists(fpath):
            return flist
        flist.append(fpath)
        return flist

    target = set(["nodes", "drpnodes"])
    if svc.scale_target is not None or len(svc.nodes) < 2 or \
       len(svc.resources_by_id) == 0:
        target.remove("nodes")
    if rcEnv.nodename in svc.nodes and len(svc.drpnodes) == 0:
        target.remove("drpnodes")
    if rcEnv.nodename in svc.drpnodes and len(svc.drpnodes) < 2:
        target.remove("drpnodes")
    if len(target) == 0:
        return

    mod = svc.load_driver("sync", "rsync")
    kwargs = {}
    src = []
    src = add_file(src, svc.paths.cf)
    src = add_file(src, svc.paths.initd)
    src = add_file(src, svc.paths.alt_initd)
    dst = os.path.join("/")
    exclude = ["--exclude=*.core"]
    kwargs["rid"] = "sync#i0"
    kwargs["src"] = src
    kwargs["dst"] = dst
    kwargs["options"] = ["-R"]+exclude
    try:
        kwargs["options"] += svc.conf_get(kwargs["rid"], "options")
    except ex.OptNotFound:
        pass
    kwargs["reset_options"] = svc.oget(kwargs["rid"], "reset_options")
    kwargs["target"] = list(target)
    kwargs["internal"] = True
    kwargs["disabled"] = get_disabled(svc, kwargs["rid"])
    kwargs["optional"] = get_optional(svc, kwargs["rid"])
    kwargs.update(sync_kwargs(svc, kwargs["rid"]))
    r = mod.SyncRsync(**kwargs)
    svc += r

def add_resources(svc):
    """
    Instanciate resource objects and add them to the service.
    Return the number of resource add errors.
    """
    ret = 0
    sections = {}
    for section in svc.cd:
        restype = section.split("#")[0]
        if restype in ("subset", "env"):
            continue
        try:
            sections[restype].add(section)
        except KeyError:
            sections[restype] = set([section])

    ordered_restypes = [
        "volume",
        "container",
        "ip",
        "disk",
        "fs",
        "share",
        "app",
        "sync",
        "task",
        "expose",
        "vhost",
        "route",
        "certificate",
        "hash_policy",
    ]

    for restype in ordered_restypes:
        for section in sections.get(restype, []):
            try:
                add_resource(svc, restype, section)
            except (ex.excError, ex.RequiredOptNotFound):
                ret += 1
    add_mandatory_syncs(svc)
    return ret

def build_services(status=None, paths=None, create_instance=False,
                   node=None):
    """
    Returns a list of all services of status matching the specified status.
    If no status is specified, returns all services.
    """
    import svc

    if paths is None:
        paths = []

    errors = []
    services = {}

    if isinstance(paths, str):
        paths = [paths]

    if len(paths) == 0:
        paths = list_services()
        missing_paths = []
    else:
        local_paths = list_services()
        missing_paths = sorted(list(set(paths) - set(local_paths)))
        for m in missing_paths:
            name, namespace, kind = split_path(m)
            if create_instance:
                services[m] = factory(kind)(name, namespace, node=node)
            else:
                # foreign service
                services[m] = factory(kind)(name, namespace, node=node, volatile=True)
        paths = list(set(paths) & set(local_paths))

    for path in paths:
        name, namespace, kind = split_path(path)
        try:
            svc = factory(kind)(name, namespace, node=node)
        except (ex.excError, ex.excInitError, ValueError, rcConfigParser.Error) as e:
            errors.append("%s: %s" % (path, str(e)))
            node.log.error(str(e))
            continue
        except ex.excAbortAction:
            continue
        except:
            import traceback
            traceback.print_exc()
            continue
        services[svc.path] = svc
    return [s for _, s in sorted(services.items())], errors


