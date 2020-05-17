from __future__ import print_function
import os
import sys
import re
import glob
import logging

from rcGlobalEnv import rcEnv
from storage import Storage
import rcLogger
import resSyncRsync
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

def standby_from_always_on(svc, section):
    always_on_opt = svc.conf_get(section, "always_on")
    if rcEnv.nodename in always_on_opt:
        return True
    if "nodes" in always_on_opt and rcEnv.nodename in svc.nodes:
        return True
    if "drpnodes" in always_on_opt and rcEnv.nodename in svc.drpnodes:
        return True
    return False

def get_sync_args(svc, s):
    kwargs = {}
    defaults = svc.cd.get("DEFAULT", {})
    kwargs["sync_max_delay"] = svc.oget(s, "sync_max_delay")
    kwargs["schedule"] = svc.oget(s, "schedule")
    return kwargs

def add_resource(svc, restype, s):
    if restype == "pool":
        restype = "zpool"
        match = "[z]{0,1}pool#"
    else:
        match = restype+"#"

    if restype in ("disk", "vg", "zpool") and re.match(match+".+pr$", s, re.I) is not None:
        # persistent reserv resource are declared by their peer resource:
        # don't add them from here
        return

    try:
        adder = globals()["add_"+restype]
    except KeyError:
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

    adder(svc, s)

def add_ip_gce(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["ipname"] = svc.oget(s, "ipname")
    kwargs["ipdev"] = svc.oget(s, "ipdev")
    kwargs["routename"] = svc.oget(s, "routename")
    kwargs["gce_zone"] = svc.oget(s, "gce_zone")
    kwargs["wait_dns"] = svc.oget(s, "wait_dns")
    ip = __import__("resIpGce")
    r = ip.Ip(**kwargs)
    svc += r

def add_ip_amazon(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["ipname"] = svc.oget(s, "ipname")
    kwargs["ipdev"] = svc.oget(s, "ipdev")
    kwargs["eip"] = svc.oget(s, "eip")
    kwargs["wait_dns"] = svc.oget(s, "wait_dns")
    ip = __import__("resIpAmazon")
    r = ip.Ip(**kwargs)
    svc += r

def add_ip(svc, s):
    """Parse the configuration file and add an ip object for each [ip#n]
    section. Ip objects are stored in a list in the service object.
    """
    try:
        rtype = svc.conf_get(s, "type")
    except ex.OptNotFound as exc:
        rtype = exc.default

    if rtype == "amazon":
        return add_ip_amazon(svc, s)
    elif rtype == "gce":
        return add_ip_gce(svc, s)

    kwargs = init_kwargs(svc, s)
    kwargs["expose"] = svc.oget(s, "expose")
    kwargs["check_carrier"] = svc.oget(s, "check_carrier")
    kwargs["alias"] = svc.oget(s, "alias")
    kwargs["ipdev"] = svc.oget(s, "ipdev")
    kwargs["wait_dns"] = svc.oget(s, "wait_dns")
    zone = svc.oget(s, "zone")

    if rtype == "cni":
        kwargs["network"] = svc.oget(s, "network")
        kwargs["netns"] = svc.oget(s, "netns")
    elif rtype in ("netns", "docker"):
        kwargs["ipname"] = svc.oget(s, "ipname")
        kwargs["mask"] = svc.oget(s, "netmask")
        kwargs["gateway"] = svc.oget(s, "gateway")
        kwargs["netns"] = svc.oget(s, "netns")
        kwargs["nsdev"] = svc.oget(s, "nsdev")
        kwargs["mode"] = svc.oget(s, "mode")
        kwargs["network"] = svc.oget(s, "network")
        kwargs["macaddr"] = svc.oget(s, "macaddr")
        kwargs["del_net_route"] = svc.oget(s, "del_net_route")
        if kwargs["mode"] == "ovs":
            kwargs["vlan_tag"] = svc.oget(s, "vlan_tag")
            kwargs["vlan_mode"] = svc.oget(s, "vlan_mode")
    elif rtype == "crossbow":
        kwargs["ipdevExt"] = svc.oget(s, "ipdevext")
        kwargs["ipname"] = svc.oget(s, "ipname")
        kwargs["mask"] = svc.oget(s, "netmask")
        kwargs["gateway"] = svc.oget(s, "gateway")
    else:
        kwargs["ipname"] = svc.oget(s, "ipname")
        kwargs["mask"] = svc.oget(s, "netmask")
        kwargs["gateway"] = svc.oget(s, "gateway")

    if rtype == "crossbow":
        if zone is not None:
            svc.log.error("'zone' and 'type=crossbow' are incompatible in section %s"%s)
            return
        ip = __import__("resIpCrossbow")
    elif zone is not None:
        kwargs["zone"] = zone
        ip = __import__("resIpZone")
    elif rtype in ("netns", "docker"):
        ip = __import__("resIpNetns"+rcEnv.sysname)
    elif rtype == "cni":
        ip = __import__("resIpCni")
    else:
        ip = __import__("resIp"+rcEnv.sysname)

    r = ip.Ip(**kwargs)
    svc += r

def add_zvol(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    try:
        mod = mimport("res", "disk", "zvol")
    except ImportError:
        svc.log.error("resDiskZvol is not implemented")
        return
    r = mod.Disk(**kwargs)
    svc += r

def add_lv(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    kwargs["vg"] = svc.oget(s, "vg")
    try:
        m = __import__("resDiskLv"+rcEnv.sysname)
    except ImportError:
        svc.log.error("resDiskLv%s is not implemented"%rcEnv.sysname)
        return
    r = m.Disk(**kwargs)
    svc += r

def add_md(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["uuid"] = svc.oget(s, "uuid")
    m = __import__("resDiskMdLinux")
    r = m.Disk(**kwargs)
    svc += r

def add_drbd(svc, s):
    """Parse the configuration file and add a drbd object for each [drbd#n]
    section. Drbd objects are stored in a list in the service object.
    """
    kwargs = init_kwargs(svc, s)
    kwargs["res"] = svc.oget(s, "res")
    mod = __import__("resDiskDrbd")
    r = mod.Drbd(**kwargs)
    svc += r

def add_vdisk(svc, s):
    kwargs = init_kwargs(svc, s)
    devpath = {}

    for attr, val in svc.cd[s].items():
        if "path@" in attr:
            devpath[attr.replace("path@", "")] = val

    if len(devpath) == 0:
        svc.log.error("path@node must be set in section %s"%s)
        return

    kwargs["devpath"] = devpath
    m = __import__("resDiskVdisk")
    r = m.Disk(**kwargs)
    svc += r

def add_loop(svc, s):
    """Parse the configuration file and add a loop object for each [loop#n]
    section. Loop objects are stored in a list in the service object.
    """
    kwargs = init_kwargs(svc, s)
    kwargs["loopFile"] = svc.oget(s, "file")

    try:
        m = __import__("resDiskLoop"+rcEnv.sysname)
    except ImportError:
        svc.log.error("resDiskLoop%s is not implemented"%rcEnv.sysname)
        return

    r = m.Disk(**kwargs)
    svc += r


def add_volume(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    kwargs["pool"] = svc.oget(s, "pool")
    kwargs["format"] = svc.oget(s, "format")
    kwargs["size"] = svc.oget(s, "size")
    kwargs["access"] = svc.oget(s, "access")
    kwargs["configs"] = svc.oget(s, "configs")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["group"] = svc.oget(s, "group")
    kwargs["perm"] = svc.oget(s, "perm")
    kwargs["dirperm"] = svc.oget(s, "dirperm")
    try:
        kwargs["secrets"] = svc.oget(s, "secrets")
    except ValueError:
        # only supported on type=shm volumes
        pass
    m = __import__("resVolume")
    r = m.Volume(**kwargs)
    svc += r

def add_disk_disk(svc, s):
    kwargs = init_kwargs(svc, s)
    m = __import__("resDiskDisk"+rcEnv.sysname)
    r = m.Disk(**kwargs)
    svc += r

def add_disk_gce(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["names"] = svc.oget(s, "names")
    kwargs["gce_zone"] = svc.oget(s, "gce_zone")
    m = __import__("resDiskGce")
    r = m.Disk(**kwargs)
    svc += r

def add_disk_amazon(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["volumes"] = svc.oget(s, "volumes")
    m = __import__("resDiskAmazon")
    r = m.Disk(**kwargs)
    svc += r

def add_disk_rados(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["images"] = svc.oget(s, "images")
    kwargs["keyring"] = svc.oget(s, "keyring")
    kwargs["client_id"] = svc.oget(s, "client_id")
    try:
        m = __import__("resDiskRados"+rcEnv.sysname)
    except ImportError:
        svc.log.error("disk type rados is not implemented")
        return

    r = m.Disk(**kwargs)
    svc += r

    # rados locking resource
    lock_shared_tag = svc.oget(s, "lock_shared_tag")
    lock = svc.oget(s, "lock")
    if not lock:
        return

    kwargs["rid"] = kwargs["rid"]+"lock"
    kwargs["lock"] = lock
    kwargs["lock_shared_tag"] = lock_shared_tag
    r = m.DiskLock(**kwargs)
    svc += r


def add_raw(svc, s):
    kwargs = init_kwargs(svc, s)
    disk_type = "Raw"+rcEnv.sysname
    kwargs["devs"] = svc.oget(s, "devs")
    zone = svc.oget(s, "zone")

    if zone is not None:
        kwargs["devs"] = set([dev.replace(":", ":<%s>" % zone) for dev in kwargs["devs"]])

    kwargs["user"] = svc.oget(s, "user")
    kwargs["group"] = svc.oget(s, "group")
    kwargs["perm"] = svc.oget(s, "perm")
    kwargs["create_char_devices"] = svc.oget(s, "create_char_devices")

    try:
        m = __import__("resDisk"+disk_type)
    except ImportError:
        svc.log.error("disk type %s driver is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    if zone is not None:
        r.tags.add("zone")
        r.tags.add(zone)
    svc += r

def add_gandi(svc, s):
    disk_type = "Gandi"
    kwargs = init_kwargs(svc, s)
    kwargs["cloud_id"] = svc.oget(s, "cloud_id")
    kwargs["name"] = svc.oget(s, "name")
    kwargs["node"] = svc.oget(s, "node")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["group"] = svc.oget(s, "group")
    kwargs["perm"] = svc.oget(s, "perm")
    try:
        m = __import__("resDisk"+disk_type)
    except ImportError:
        svc.log.error("disk type %s is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    svc += r

def add_disk_compat(svc, s):
    try:
        disk_type = svc.conf_get(s, "type")
    except ex.OptNotFound as exc:
        disk_type = s.split("#")[0]
    if len(disk_type) >= 2:
        disk_type = disk_type[0].upper() + disk_type[1:].lower()

    if disk_type == "Drbd":
        add_drbd(svc, s)
        return
    if disk_type == "Vdisk":
        add_vdisk(svc, s)
        return
    if disk_type == "Vmdg":
        add_vmdg(svc, s)
        return
    if disk_type == "Pool":
        add_zpool(svc, s)
        return
    if disk_type == "Zpool":
        add_zpool(svc, s)
        return
    if disk_type == "Loop":
        add_loop(svc, s)
        return
    if disk_type == "Md":
        add_md(svc, s)
        return
    if disk_type == "Zvol":
        add_zvol(svc, s)
        return
    if disk_type == "Lv":
        add_lv(svc, s)
        return
    if disk_type == "Gce":
        add_disk_gce(svc, s)
        return
    if disk_type == "Disk":
        add_disk_disk(svc, s)
        return
    if disk_type == "Amazon":
        add_disk_amazon(svc, s)
        return
    if disk_type == "Rados":
        add_disk_rados(svc, s)
        return
    if disk_type == "Raw":
        add_raw(svc, s)
        return
    if disk_type == "Gandi":
        add_gandi(svc, s)
        return
    if disk_type in ("Veritas", "Vxdg"):
        add_vxdg(svc, s)
        return
    if disk_type == "Vxvol":
        add_vxvol(svc, s)
        return

    raise ex.OptNotFound

def add_vxdg(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    try:
        m = __import__("resDiskVxdg")
    except ImportError:
        svc.log.error("disk type vxdg is not implemented")
        return
    r = m.Disk(**kwargs)
    svc += r

def add_vxvol(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    kwargs["vg"] = svc.oget(s, "vg")
    try:
        m = __import__("resDiskVxvol")
    except ImportError:
        svc.log.error("disk type vxvol is not implemented")
        return
    r = m.Disk(**kwargs)
    svc += r

def add_vg(svc, s):
    try:
        add_disk_compat(svc, s)
        return
    except ex.OptNotFound as exc:
        pass

    disk_type = rcEnv.sysname
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    kwargs["dsf"] = svc.oget(s, "dsf")
    try:
        m = __import__("resDiskVg"+disk_type)
    except ImportError:
        svc.log.error("disk type %s is not implemented"%disk_type)
        return
    r = m.Disk(**kwargs)
    svc += r

def add_sync(svc, s):
    rtype = svc.oget(s, "type")
    globals()["add_sync_"+rtype](svc, s)

def add_container(svc, s):
    rtype = svc.oget(s, "type")
    if rtype == "oci":
        rtype = svc.node.oci
    try:
        globals()["add_container_"+rtype](svc, s)
    except KeyError:
        raise ex.excError("no container.%s driver" % rtype)

def add_disk(svc, s):
    """Parse the configuration file and add a disk object for each [disk#n]
    section. Disk objects are stored in a list in the service object.
    """
    try:
        disk_type = svc.conf_get(s, "type")
    except ex.OptNotFound as exc:
        disk_type = s.split("#")[0]

    if len(disk_type) >= 2:
        disk_type = disk_type[0].upper() + disk_type[1:].lower()

    if disk_type == "Drbd":
        add_drbd(svc, s)
        return
    if disk_type == "Vdisk":
        add_vdisk(svc, s)
        return
    if disk_type == "Vmdg":
        add_vmdg(svc, s)
        return
    if disk_type == "Pool":
        add_zpool(svc, s)
        return
    if disk_type == "Zpool":
        add_zpool(svc, s)
        return
    if disk_type == "Loop":
        add_loop(svc, s)
        return
    if disk_type == "Zvol":
        add_zvol(svc, s)
        return
    if disk_type == "Lv":
        add_lv(svc, s)
        return
    if disk_type == "Md":
        add_md(svc, s)
        return
    if disk_type == "Gce":
        add_disk_gce(svc, s)
        return
    if disk_type == "Disk":
        add_disk_disk(svc, s)
        return
    if disk_type == "Amazon":
        add_disk_amazon(svc, s)
        return
    if disk_type == "Rados":
        add_disk_rados(svc, s)
        return
    if disk_type == "Raw":
        add_raw(svc, s)
        return
    if disk_type == "Gandi":
        add_gandi(svc, s)
        return
    if disk_type in ("Veritas", "Vxdg"):
        add_vxdg(svc, s)
        return
    if disk_type == "Vxvol":
        add_vxvol(svc, s)
        return
    if disk_type == "Lvm" or disk_type == "Vg" or disk_type == rcEnv.sysname:
        add_vg(svc, s)
        return

def add_vmdg(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["container_id"] = svc.oget(s, "container_id")

    if not kwargs["container_id"] in svc.cd:
        svc.log.error("%s.container_id points to an invalid section"%kwargs["container_id"])
        return

    try:
        container_type = svc.conf_get(kwargs["container_id"], "type")
    except ex.OptNotFound as exc:
        svc.log.error("type must be set in section %s"%kwargs["container_id"])
        return

    if container_type == "ldom":
        m = __import__("resDiskLdom")
    else:
        return

    r = m.Disk(**kwargs)
    svc += r

def add_zpool(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    kwargs["multihost"] = svc.oget(s, "multihost")
    zone = svc.oget(s, "zone")
    mod = mimport("res", "disk", "zpool")
    r = mod.Disk(**kwargs)
    if zone is not None:
        r.tags.add("zone")
        r.tags.add(zone)
    svc += r

def add_vhost(svc, s):
    _type = svc.oget(s, "type")
    fname = "add_vhost_"+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s))
    globals()[fname](svc, s)

def add_vhost_envoy(svc, s):
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "envoy"))
    try:
        m = __import__("resVhostEnvoy")
    except ImportError:
        svc.log.error("resVhostEnvoy is not implemented")
        return

    r = m.Vhost(**kwargs)
    svc += r

def add_route(svc, s):
    _type = svc.oget(s, "type")
    fname = "add_route_"+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s))
    globals()[fname](svc, s)

def add_route_envoy(svc, s):
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "envoy"))
    try:
        m = __import__("resRouteEnvoy")
    except ImportError:
        svc.log.error("resRouteEnvoy is not implemented")
        return
    r = m.Route(**kwargs)
    svc += r

def add_hash_policy(svc, s):
    _type = svc.oget(s, "type")
    fname = "add_hash_policy_"+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s))
    globals()[fname](svc, s)

def add_hash_policy_envoy(svc, s):
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "envoy"))
    try:
        m = __import__("resHashpolicyEnvoy")
    except ImportError:
        svc.log.error("resHashpolicyEnvoy is not implemented")
        return
    r = m.Hashpolicy(**kwargs)
    svc += r

def add_expose(svc, s):
    _type = svc.oget(s, "type")
    fname = "add_expose_"+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s))
    globals()[fname](svc, s)

def add_expose_envoy(svc, s):
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "envoy"))
    try:
        m = __import__("resExposeEnvoy")
    except ImportError:
        svc.log.error("resExposeEnvoy is not implemented")
        return
    r = m.Expose(**kwargs)
    svc += r

def add_certificate(svc, s):
    rtype = svc.oget(s, "type")
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "tls"))
    try:
        mod = mimport("res", "certificate", rtype)
    except ImportError:
        svc.log.error("certificate.%s driver is not implemented" % rtype)
        return
    r = mod.Certificate(**kwargs)
    svc += r

def add_share(svc, s):
    _type = svc.oget(s, "type")
    fname = "add_share_"+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s))
    globals()[fname](svc, s)

def add_share_nfs(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["path"] = svc.oget(s, "path")
    kwargs["opts"] = svc.oget(s, "opts")

    try:
        m = __import__("resShareNfs"+rcEnv.sysname)
    except ImportError:
        svc.log.error("resShareNfs%s is not implemented"%rcEnv.sysname)
        return

    r = m.Share(**kwargs)
    svc += r

def add_fs_flag(svc, s):
    kwargs = init_kwargs(svc, s)
    mod = mimport("res", "fs", "flag")
    if not hasattr(mod, 'Fs'):
        svc.log.error("type 'flag' in section fs is not implemented on %s" % rcEnv.sysname)
        raise ex.excError
    r = mod.Fs(**kwargs)
    svc += r

def add_fs_docker(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["driver"] = svc.oget(s, "driver")
    kwargs["options"] = svc.oget(s, "options")
    m = __import__("resFsDocker")
    r = m.Fs(**kwargs)
    svc += r

def add_fs_directory(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["path"] = svc.oget(s, "path")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["group"] = svc.oget(s, "group")
    kwargs["perm"] = svc.oget(s, "perm")
    zone = svc.oget(s, "zone")

    if zone is not None:
        zp = None
        for r in [r for r in svc.resources_by_id.values() if r.type == "container.zone"]:
            if r.name == zone:
                try:
                    zp = r.zonepath
                except:
                    zp = "<%s>" % zone
                break
        if zp is None:
            svc.log.error("zone %s, referenced in %s, not found"%(zone, s))
            raise ex.excError()
        kwargs["path"] = zp+"/root"+kwargs["path"]
        if "<%s>" % zone != zp:
            kwargs["path"] = os.path.realpath(kwargs["path"])

    mod = __import__("resFsDir")
    r = mod.FsDir(**kwargs)

    if zone is not None:
        r.tags.add(zone)
        r.tags.add("zone")

    svc += r

def add_fs(svc, s):
    """Parse the configuration file and add a fs object for each [fs#n]
    section. Fs objects are stored in a list in the service object.
    """
    kwargs = init_kwargs(svc, s)

    try:
        kwargs["fs_type"] = svc.conf_get(s, "type")
    except ex.OptNotFound as exc:
        kwargs["fs_type"] = ""

    if kwargs["fs_type"] == "directory":
        add_fs_directory(svc, s)
        return

    if kwargs["fs_type"] == "docker":
        add_fs_docker(svc, s)
        return

    if kwargs["fs_type"] == "flag":
        add_fs_flag(svc, s)
        return

    kwargs["device"] = svc.oget(s, "dev")
    kwargs["mount_point"] = svc.oget(s, "mnt")
    kwargs["stat_timeout"] = svc.oget(s, "stat_timeout")

    if kwargs["mount_point"] and kwargs["mount_point"][-1] != "/" and kwargs["mount_point"][-1] == "/":
        # Remove trailing / to not risk losing rsync src trailing / upon snap
        # mountpoint substitution.
        kwargs["mount_point"] = kwargs["mount_point"][0:-1]

    try:
        kwargs["mount_options"] = svc.conf_get(s, "mnt_opt")
    except ex.OptNotFound as exc:
        kwargs["mount_options"] = ""

    try:
        kwargs["snap_size"] = svc.conf_get(s, "snap_size")
    except ex.OptNotFound as exc:
        pass

    zone = svc.oget(s, "zone")

    if zone is not None:
        zp = None
        for r in [r for r in svc.resources_by_id.values() if r.type == "container.zone"]:
            if r.name == zone:
                try:
                    zp = r.zonepath
                except:
                    zp = "<%s>" % zone
                break
        if zp is None:
            svc.log.error("zone %s, referenced in %s, not found"%(zone, s))
            raise ex.excError()
        kwargs["mount_point"] = zp+"/root"+kwargs["mount_point"]
        if "<%s>" % zone != zp:
            kwargs["mount_point"] = os.path.realpath(kwargs["mount_point"])

    try:
        mount = __import__("resFs"+rcEnv.sysname)
    except ImportError:
        svc.log.error("resFs%s is not implemented"%rcEnv.sysname)
        return

    r = mount.Mount(**kwargs)

    if zone is not None:
        r.tags.add(zone)
        r.tags.add("zone")

    svc += r

def container_kwargs(svc, s, default_name="name"):
    """
    Common kwargs for all containers.
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

def add_container_esx(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    m = __import__("resContainerEsx")
    r = m.Esx(**kwargs)
    svc += r

def add_container_hpvm(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    m = __import__("resContainerHpVm")
    r = m.HpVm(**kwargs)
    svc += r

def add_container_ldom(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    m = __import__("resContainerLdom")
    r = m.Ldom(**kwargs)
    svc += r

def add_container_vbox(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["headless"] = svc.oget(s, "headless")
    m = __import__("resContainerVbox")
    r = m.Vbox(**kwargs)
    svc += r

def add_container_xen(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    m = __import__("resContainerXen")
    r = m.Xen(**kwargs)
    svc += r

def add_container_zone(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["delete_on_stop"] = svc.oget(s, "delete_on_stop")
    m = __import__("resContainerZone")
    r = m.Zone(**kwargs)
    svc += r

def add_container_vcloud(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["cloud_id"] = svc.oget(s, "cloud_id")
    kwargs["vapp"] = svc.oget(s, "vapp")
    kwargs["key_name"] = svc.oget(s, "key_name")
    m = __import__("resContainerVcloud")
    r = m.CloudVm(**kwargs)
    svc += r

def add_container_amazon(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["cloud_id"] = svc.oget(s, "cloud_id")
    kwargs["key_name"] = svc.oget(s, "key_name")

    # provisioning keywords
    kwargs["image_id"] = svc.oget(s, "image_id")
    kwargs["size"] = svc.oget(s, "size")
    kwargs["subnet"] = svc.oget(s, "subnet")
    m = __import__("resContainerAmazon")
    r = m.CloudVm(**kwargs)
    svc += r

def add_container_openstack(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["cloud_id"] = svc.oget(s, "cloud_id")
    kwargs["key_name"] = svc.oget(s, "key_name")
    kwargs["size"] = svc.oget(s, "size")
    kwargs["shared_ip_group"] = svc.oget(s, "shared_ip_group")
    m = __import__("resContainerOpenstack")
    r = m.CloudVm(**kwargs)
    svc += r

def add_container_vz(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    m = __import__("resContainerVz")
    r = m.Vz(**kwargs)
    svc += r

def add_container_kvm(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    m = __import__("resContainerKvm")
    r = m.Kvm(**kwargs)
    svc += r

def add_container_srp(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    m = __import__("resContainerSrp")
    r = m.Srp(**kwargs)
    svc += r

def add_container_lxd(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    m = __import__("resContainerLxd")
    r = m.Container(**kwargs)
    svc += r

def add_container_lxc(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["rcmd"] = get_rcmd(svc, s)
    kwargs["cf"] = svc.oget(s, "cf")
    kwargs["container_data_dir"] = svc.oget(s, "container_data_dir")
    m = __import__("resContainerLxc")
    r = m.Lxc(**kwargs)
    svc += r

def add_container_docker(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s, default_name=None))
    kwargs["image"] = svc.oget(s, "image")
    kwargs["image_pull_policy"] = svc.oget(s, "image_pull_policy")
    kwargs["run_command"] = svc.oget(s, "command")
    kwargs["run_args"] = svc.oget(s, "run_args")
    kwargs["rm"] = svc.oget(s, "rm")
    kwargs["detach"] = svc.oget(s, "detach")
    kwargs["entrypoint"] = svc.oget(s, "entrypoint")
    kwargs["netns"] = svc.oget(s, "netns")
    kwargs["userns"] = svc.oget(s, "userns")
    kwargs["pidns"] = svc.oget(s, "pidns")
    kwargs["ipcns"] = svc.oget(s, "ipcns")
    kwargs["utsns"] = svc.oget(s, "utsns")
    kwargs["privileged"] = svc.oget(s, "privileged")
    kwargs["interactive"] = svc.oget(s, "interactive")
    kwargs["tty"] = svc.oget(s, "tty")
    kwargs["volume_mounts"] = svc.oget(s, "volume_mounts")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    kwargs["devices"] = svc.oget(s, "devices")
    m = __import__("resContainerDocker")
    r = m.Container(**kwargs)
    svc += r

def add_container_podman(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s, default_name=None))
    kwargs["image"] = svc.oget(s, "image")
    kwargs["image_pull_policy"] = svc.oget(s, "image_pull_policy")
    kwargs["run_command"] = svc.oget(s, "command")
    kwargs["run_args"] = svc.oget(s, "run_args")
    kwargs["rm"] = svc.oget(s, "rm")
    kwargs["detach"] = svc.oget(s, "detach")
    kwargs["entrypoint"] = svc.oget(s, "entrypoint")
    kwargs["netns"] = svc.oget(s, "netns")
    kwargs["userns"] = svc.oget(s, "userns")
    kwargs["pidns"] = svc.oget(s, "pidns")
    kwargs["ipcns"] = svc.oget(s, "ipcns")
    kwargs["utsns"] = svc.oget(s, "utsns")
    kwargs["privileged"] = svc.oget(s, "privileged")
    kwargs["interactive"] = svc.oget(s, "interactive")
    kwargs["tty"] = svc.oget(s, "tty")
    kwargs["volume_mounts"] = svc.oget(s, "volume_mounts")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    kwargs["devices"] = svc.oget(s, "devices")
    m = __import__("resContainerPodman")
    r = m.Container(**kwargs)
    svc += r

def add_container_ovm(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["uuid"] = svc.oget(s, "uuid")
    m = __import__("resContainerOvm")
    r = m.Ovm(**kwargs)
    svc += r

def add_container_jail(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["jailroot"] = svc.oget(s, "jailroot")
    kwargs["ips"] = svc.oget(s, "ips")
    kwargs["ip6s"] = svc.oget(s, "ip6s")
    m = __import__("resContainerJail")
    r = m.Jail(**kwargs)
    svc += r

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
    kwargs.update(get_sync_args(svc, kwargs["rid"]))
    r = resSyncRsync.Rsync(**kwargs)
    svc += r

def add_sync_docker(svc, s):
    kwargs = {}

    kwargs["target"] = svc.oget(s, "target")

    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    m = __import__("resSyncDocker")
    r = m.SyncDocker(**kwargs)
    svc += r

def add_sync_btrfs(svc, s):
    kwargs = {}

    kwargs["src"] = svc.oget(s, "src")
    kwargs["dst"] = svc.oget(s, "dst")
    kwargs["target"] = svc.oget(s, "target")
    kwargs["recursive"] = svc.oget(s, "recursive")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    btrfs = __import__("resSyncBtrfs")
    r = btrfs.SyncBtrfs(**kwargs)
    svc += r

def add_sync_zfs(svc, s):
    kwargs = {}

    kwargs["src"] = svc.oget(s, "src")
    kwargs["dst"] = svc.oget(s, "dst")
    kwargs["target"] = svc.oget(s, "target")
    kwargs["recursive"] = svc.oget(s, "recursive")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    zfs = __import__("resSyncZfs")
    r = zfs.SyncZfs(**kwargs)
    svc += r

def add_sync_dds(svc, s):
    kwargs = {}

    kwargs["src"] = svc.oget(s, "src")
    kwargs["target"] = svc.oget(s, "target")

    dsts = {}
    for node in svc.nodes | svc.drpnodes:
        dst = svc.oget(s, "dst", impersonate=node)
        dsts[node] = dst

    if len(dsts) == 0:
        for node in svc.nodes | svc.drpnodes:
            dsts[node] = kwargs["src"]

    kwargs["dsts"] = dsts
    kwargs["snap_size"] = svc.oget(s, "snap_size")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    dds = __import__("resSyncDds")
    r = dds.syncDds(**kwargs)
    svc += r

def add_sync_s3(svc, s):
    kwargs = {}
    kwargs["full_schedule"] = svc.oget(s, "full_schedule")
    kwargs["options"] = svc.oget(s, "options")
    kwargs["snar"] = svc.oget(s, "snar")
    kwargs["bucket"] = svc.oget(s, "bucket")
    kwargs["src"] = svc.oget(s, "src")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__("resSyncS3")
    r = sc.syncS3(**kwargs)
    svc += r

def add_sync_zfssnap(svc, s):
    kwargs = {}
    kwargs["name"] = svc.oget(s, "name")
    kwargs["keep"] = svc.oget(s, "keep")
    kwargs["recursive"] = svc.oget(s, "recursive")
    kwargs["dataset"] = svc.oget(s, "dataset")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__("resSyncZfsSnap")
    r = sc.syncZfsSnap(**kwargs)
    svc += r

def add_sync_btrfssnap(svc, s):
    kwargs = {}
    kwargs["name"] = svc.oget(s, "name")
    kwargs["keep"] = svc.oget(s, "keep")
    kwargs["subvol"] = svc.oget(s, "subvol")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__("resSyncBtrfsSnap")
    r = sc.syncBtrfsSnap(**kwargs)
    svc += r

def add_sync_necismsnap(svc, s):
    kwargs = {}
    kwargs["array"] = svc.oget(s, "array")
    kwargs["devs"] = svc.oget(s, "devs")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "nec", "ism", "snap")
    r = mod.syncNecIsmSnap(**kwargs)
    svc += r

def add_sync_evasnap(svc, s):
    kwargs = {}
    kwargs["eva_name"] = svc.oget(s, "eva_name")
    kwargs["snap_name"] = svc.oget(s, "snap_name")
    import json
    try:
        pairs = json.loads(svc.oget(s, "pairs"))
    except:
        pairs = None
    if not pairs:
        svc.log.error("config file section %s must have pairs set" % s)
        return
    else:
        kwargs["pairs"] = pairs

    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "evasnap")
    r = mod.syncEvasnap(**kwargs)
    svc += r

def add_sync_hp3parsnap(svc, s):
    kwargs = {}

    kwargs["array"] = svc.oget(s, "array")
    vv_names = svc.oget(s, "vv_names")

    if len(vv_names) == 0:
        svc.log.error("config file section %s must have at least one vv_name set" % s)
        return

    kwargs["vv_names"] = vv_names

    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "hp3par", "snap")
    r = mod.syncHp3parSnap(**kwargs)
    svc += r

def add_sync_hp3par(svc, s):
    kwargs = {}

    kwargs["mode"] = svc.oget(s, "mode")
    kwargs["array"] = svc.oget(s, "array")

    rcg_names = {}
    for node in svc.nodes | svc.drpnodes:
        array = svc.oget(s, "array", impersonate=node)
        rcg = svc.oget(s, "rcg", impersonate=node)
        rcg_names[array] = rcg

    if len(rcg_names) == 0:
        svc.log.error("config file section %s must have rcg set" % s)
        return

    kwargs["rcg_names"] = rcg_names

    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "hp3par")
    r = mod.syncHp3par(**kwargs)
    svc += r

def add_sync_symsrdfs(svc, s):
    kwargs = {}

    kwargs["symdg"] = svc.oget(s, "symdg")
    kwargs["rdfg"] = svc.oget(s, "rdfg")
    kwargs["symid"] = svc.oget(s, "symid")

    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "sym", "srdf", "s")
    r = mod.syncSymSrdfS(**kwargs)
    svc += r


def add_sync_radosclone(svc, s):
    kwargs = {}
    kwargs["client_id"] = svc.oget(s, "client_id")
    kwargs["keyring"] = svc.oget(s, "keyring")
    kwargs["pairs"] = svc.oget(s, "pairs")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "rados")
    r = mod.syncRadosClone(**kwargs)
    svc += r

def add_sync_radossnap(svc, s):
    kwargs = {}
    kwargs["client_id"] = svc.oget(s, "client_id")
    kwargs["keyring"] = svc.oget(s, "keyring")
    kwargs["images"] = svc.oget(s, "images")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "rados")
    r = mod.syncRadosSnap(**kwargs)
    svc += r

def add_sync_symsnap(svc, s):
    _add_sync_symclone(svc, s, "sync.symsnap")

def add_sync_symclone(svc, s):
    _add_sync_symclone(svc, s, "sync.symclone")

def _add_sync_symclone(svc, s, t):
    kwargs = {}
    kwargs["type"] = t
    kwargs["pairs"] = svc.oget(s, "pairs")
    kwargs["symid"] = svc.oget(s, "symid")
    kwargs["recreate_timeout"] = svc.oget(s, "recreate_timeout")
    kwargs["restore_timeout"] = svc.oget(s, "restore_timeout")
    kwargs["consistent"] = svc.oget(s, "consistent")
    kwargs["precopy"] = svc.oget(s, "precopy")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "symclone")
    r = mod.syncSymclone(**kwargs)
    svc += r

def add_sync_ibmdssnap(svc, s):
    kwargs = {}

    kwargs["pairs"] = svc.oget(s, "pairs")
    kwargs["array"] = svc.oget(s, "array")
    kwargs["bgcopy"] = svc.oget(s, "bgcopy")
    kwargs["recording"] = svc.oget(s, "recording")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    mod = mimport("res", "sync", "ibmds", "snap")
    r = mod.syncIbmdsSnap(**kwargs)
    svc += r

def add_sync_nexenta(svc, s):
    kwargs = {}

    kwargs["name"] = svc.oget(s, "name")
    kwargs["path"] = svc.oget(s, "path")
    kwargs["reversible"] = svc.oget(s, "reversible")

    filers = {}
    for n in svc.nodes | svc.drpnodes:
        filers[n] = svc.oget(s, "filer", impersonate=n)

    kwargs["filers"] = filers
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    import resSyncNexenta
    r = resSyncNexenta.syncNexenta(**kwargs)
    svc += r

def add_sync_netapp(svc, s):
    kwargs = {}

    kwargs["path"] = svc.oget(s, "path")
    kwargs["user"] = svc.oget(s, "user")

    filers = {}
    for n in svc.nodes | svc.drpnodes:
        filers[n] = svc.oget(s, "filer", impersonate=n)

    kwargs["filers"] = filers
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    import resSyncNetapp
    r = resSyncNetapp.syncNetapp(**kwargs)
    svc += r

def add_sync_rsync(svc, s):
    if s.startswith("sync#i"):
        # internal syncs have their own dedicated add function
        return

    kwargs = {}
    kwargs["src"] = []
    _s = svc.oget(s, "src")
    for src in _s:
        kwargs["src"] += glob.glob(src)

    kwargs["dst"] = svc.oget(s, "dst")
    kwargs["options"] = svc.oget(s, "options")
    kwargs["reset_options"] = svc.oget(s, "reset_options")
    kwargs["dstfs"] = svc.oget(s, "dstfs")
    kwargs["snap"] = svc.oget(s, "snap")
    kwargs["bwlimit"] = svc.oget(s, "bwlimit")
    kwargs["target"] = svc.oget(s, "target")
    kwargs["rid"] = s
    kwargs["subset"] = get_subset(svc, s)
    kwargs["tags"] = get_tags(svc, s)
    kwargs["disabled"] = get_disabled(svc, s)
    kwargs["optional"] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    r = resSyncRsync.Rsync(**kwargs)
    svc += r

def add_task(svc, s):
    rtype = svc.oget(s, "type")
    if rtype == "oci":
        rtype = svc.node.oci
    if rtype == "docker":
        add_task_docker(svc, s)
    elif rtype == "podman":
        add_task_podman(svc, s)
    elif rtype == "host":
        add_task_host(svc, s)

def add_task_podman(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["image"] = svc.oget(s, "image")
    kwargs["image_pull_policy"] = svc.oget(s, "image_pull_policy")
    kwargs["run_command"] = svc.oget(s, "command")
    kwargs["run_args"] = svc.oget(s, "run_args")
    kwargs["rm"] = svc.oget(s, "rm")
    kwargs["entrypoint"] = svc.oget(s, "entrypoint")
    kwargs["netns"] = svc.oget(s, "netns")
    kwargs["userns"] = svc.oget(s, "userns")
    kwargs["pidns"] = svc.oget(s, "pidns")
    kwargs["ipcns"] = svc.oget(s, "ipcns")
    kwargs["utsns"] = svc.oget(s, "utsns")
    kwargs["privileged"] = svc.oget(s, "privileged")
    kwargs["interactive"] = svc.oget(s, "interactive")
    kwargs["tty"] = svc.oget(s, "tty")
    kwargs["volume_mounts"] = svc.oget(s, "volume_mounts")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    kwargs["devices"] = svc.oget(s, "devices")
    kwargs["command"] = svc.oget(s, "command")
    kwargs["on_error"] = svc.oget(s, "on_error")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["timeout"] = svc.oget(s, "timeout")
    kwargs["snooze"] = svc.oget(s, "snooze")
    kwargs["log"] = svc.oget(s, "log")
    kwargs["confirmation"] = svc.oget(s, "confirmation")
    kwargs["check"] = svc.oget(s, "check")
    import resTaskPodman
    r = resTaskPodman.Task(**kwargs)
    svc += r

def add_task_docker(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["image"] = svc.oget(s, "image")
    kwargs["image_pull_policy"] = svc.oget(s, "image_pull_policy")
    kwargs["run_command"] = svc.oget(s, "command")
    kwargs["run_args"] = svc.oget(s, "run_args")
    kwargs["rm"] = svc.oget(s, "rm")
    kwargs["entrypoint"] = svc.oget(s, "entrypoint")
    kwargs["netns"] = svc.oget(s, "netns")
    kwargs["userns"] = svc.oget(s, "userns")
    kwargs["pidns"] = svc.oget(s, "pidns")
    kwargs["ipcns"] = svc.oget(s, "ipcns")
    kwargs["utsns"] = svc.oget(s, "utsns")
    kwargs["privileged"] = svc.oget(s, "privileged")
    kwargs["interactive"] = svc.oget(s, "interactive")
    kwargs["tty"] = svc.oget(s, "tty")
    kwargs["volume_mounts"] = svc.oget(s, "volume_mounts")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    kwargs["devices"] = svc.oget(s, "devices")
    kwargs["command"] = svc.oget(s, "command")
    kwargs["on_error"] = svc.oget(s, "on_error")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["timeout"] = svc.oget(s, "timeout")
    kwargs["snooze"] = svc.oget(s, "snooze")
    kwargs["log"] = svc.oget(s, "log")
    kwargs["confirmation"] = svc.oget(s, "confirmation")
    kwargs["check"] = svc.oget(s, "check")
    import resTaskDocker
    r = resTaskDocker.Task(**kwargs)
    svc += r

def add_task_host(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["command"] = svc.oget(s, "command")
    kwargs["on_error"] = svc.oget(s, "on_error")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["timeout"] = svc.oget(s, "timeout")
    kwargs["snooze"] = svc.oget(s, "snooze")
    kwargs["log"] = svc.oget(s, "log")
    kwargs["confirmation"] = svc.oget(s, "confirmation")
    kwargs["check"] = svc.oget(s, "check")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    import resTaskHost
    r = resTaskHost.Task(**kwargs)
    svc += r

def add_app_winservice(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    kwargs["timeout"] = svc.oget(s, "timeout")
    kwargs["start_timeout"] = svc.oget(s, "start_timeout")
    kwargs["stop_timeout"] = svc.oget(s, "stop_timeout")
    mod = mimport("res", "app", "winservice")
    r = mod.App(**kwargs)
    svc += r

def add_app(svc, s):
    rtype = svc.oget(s, "type")

    if rtype == "winservice":
        return add_app_winservice(svc, s)

    kwargs = init_kwargs(svc, s)
    kwargs["script"] = svc.oget(s, "script")
    kwargs["start"] = svc.oget(s, "start")
    kwargs["stop"] = svc.oget(s, "stop")
    kwargs["check"] = svc.oget(s, "check")
    kwargs["info"] = svc.oget(s, "info")
    kwargs["status_log"] = svc.oget(s, "status_log")
    kwargs["timeout"] = svc.oget(s, "timeout")
    kwargs["start_timeout"] = svc.oget(s, "start_timeout")
    kwargs["stop_timeout"] = svc.oget(s, "stop_timeout")
    kwargs["check_timeout"] = svc.oget(s, "check_timeout")
    kwargs["info_timeout"] = svc.oget(s, "info_timeout")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["group"] = svc.oget(s, "group")
    kwargs["cwd"] = svc.oget(s, "cwd")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    kwargs["retcodes"] = svc.oget(s, "retcodes")

    if rtype == "simple":
        kwargs["kill"] = svc.oget(s, "kill")

    mod = mimport("res", "app", rtype)
    r = mod.App(**kwargs)
    svc += r


def add_resources(svc):
    """
    Instanciate resource objects and add them to the service.
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


