from __future__ import print_function
import os
import sys
import re
import glob

from rcGlobalEnv import rcEnv, Storage
from rcNode import discover_node
import rcLogger
import resSyncRsync
import rcExceptions as ex
import rcConfigParser
from rcUtilities import cmdline2list, ximport, check_privs

if 'PATH' not in os.environ:
    os.environ['PATH'] = ""
os.environ['LANG'] = 'C'
os.environ['PATH'] += ':/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'

def get_tags(svc, section):
    try:
        s = svc.conf_get(section, 'tags')
    except ex.OptNotFound as exc:
        s = exc.default
    return s

def get_optional(svc, section):
    try:
        return svc.conf_get(section, "optional")
    except ex.OptNotFound as exc:
        return exc.default

def get_monitor(svc, section):
    try:
        return svc.conf_get(section, "monitor")
    except ex.OptNotFound as exc:
        return exc.default

def get_rcmd(svc, section):
    try:
        return svc.conf_get(section, 'rcmd')
    except ex.OptNotFound as exc:
        return exc.default

def get_subset(svc, section):
    try:
        return svc.conf_get(section, 'subset')
    except ex.OptNotFound as exc:
        return exc.default

def get_osvc_root_path(svc, section):
    try:
        return svc.conf_get(section, 'osvc_root_path')
    except ex.OptNotFound as exc:
        return exc.default
    return

def get_restart(svc, section):
    try:
        return svc.conf_get(section, 'restart')
    except ex.OptNotFound as exc:
        return exc.default

def get_disabled(svc, section):
    try:
        return svc.conf_get(section, 'disable')
    except ex.OptNotFound as exc:
        return exc.default

def always_on_nodes_set(svc, section):
    try:
        always_on_opt = svc.conf_get(section, "always_on")
    except ex.OptNotFound as exc:
        always_on_opt = exc.default
    always_on = set([])
    if 'nodes' in always_on_opt:
        always_on |= svc.nodes
    if 'drpnodes' in always_on_opt:
        always_on |= svc.drpnodes
    always_on |= set(always_on_opt) - set(['nodes', 'drpnodes'])
    return always_on

def get_sync_args(svc, s):
    kwargs = {}
    defaults = svc.config.defaults()

    if svc.config.has_option(s, 'sync_max_delay'):
        kwargs['sync_max_delay'] = svc.conf_get(s, 'sync_max_delay')
    elif 'sync_max_delay' in defaults:
        kwargs['sync_max_delay'] = svc.conf_get('DEFAULT', 'sync_max_delay')

    if svc.config.has_option(s, 'schedule'):
        kwargs['schedule'] = svc.conf_get(s, 'schedule')
    elif svc.config.has_option(s, 'period') or svc.config.has_option(s, 'sync_period'):
        # old schedule syntax compatibility
        from rcScheduler import Scheduler
        kwargs['schedule'] = Scheduler().sched_convert_to_schedule(svc.config, s, prefix='sync_')
    elif 'sync_schedule' in defaults:
        kwargs['schedule'] = svc.conf_get('DEFAULT', 'sync_schedule')
    elif 'sync_period' in defaults:
        # old schedule syntax compatibility for internal sync
        from rcScheduler import Scheduler
        kwargs['schedule'] = Scheduler().sched_convert_to_schedule(svc.config, s, prefix='sync_')

    return kwargs

def add_resources(svc, restype):
    for s in svc.config.sections():
        try:
            add_resource(svc, restype, s)
        except ex.RequiredOptNotFound:
            continue

def add_resource(svc, restype, s):
    if restype == "pool":
        restype = "zpool"
        match = "[z]{0,1}pool#"
    else:
        match = restype+"#"

    if restype in ("disk", "vg", "zpool") and re.match(match+'.+pr', s, re.I) is not None:
        # persistent reserv resource are declared by their peer resource:
        # don't add them from here
        return

    if s != 'app' and s != restype and re.match(match, s, re.I) is None:
        return

    tags = get_tags(svc, s)

    if svc.encap and 'encap' not in tags:
        return

    if not svc.encap and 'encap' in tags:
        svc.has_encap_resources = True
        try:
            subset = svc.conf_get(s, 'subset')
        except ex.OptNotFound as exc:
            subset = exc.default
        svc.encap_resources[s] = Storage({
            "rid": s,
            "tags": tags,
            "subset": subset,
        })
        return

    if s in svc.resources_by_id:
        return

    globals()['add_'+restype](svc, s)

def add_ip_gce(svc, s):
    kwargs = {}

    try:
        rtype = svc.conf_get(s, 'type')
    except ex.OptNotFound as exc:
        rtype = exc.default

    if rtype != "gce":
        return

    kwargs['ipname'] = svc.conf_get(s, 'ipname')
    kwargs['ipdev'] = svc.conf_get(s, 'ipdev')

    try:
        kwargs['routename'] = svc.conf_get(s, 'routename')
    except ex.OptNotFound as exc:
        kwargs['routename'] = exc.default

    try:
        kwargs['gce_zone'] = svc.conf_get(s, 'gce_zone')
    except ex.OptNotFound as exc:
        kwargs['gce_zone'] = exc.default

    ip = __import__('resIpGce')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    r = ip.Ip(**kwargs)
    svc += r

def add_ip_amazon(svc, s):
    kwargs = {}

    try:
        rtype = svc.conf_get(s, 'type')
    except ex.OptNotFound as exc:
        rtype = exc.default

    if rtype != "amazon":
        return

    kwargs['ipname'] = svc.conf_get(s, 'ipname')
    kwargs['ipdev'] = svc.conf_get(s, 'ipdev')

    try:
        kwargs['eip'] = svc.conf_get(s, 'eip')
    except ex.OptNotFound as exc:
        kwargs['eip'] = None

    ip = __import__('resIpAmazon')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    r = ip.Ip(**kwargs)
    svc += r

def add_ip(svc, s):
    """Parse the configuration file and add an ip object for each [ip#n]
    section. Ip objects are stored in a list in the service object.
    """
    try:
        rtype = svc.conf_get(s, 'type')
    except ex.OptNotFound as exc:
        rtype = exc.default

    if rtype == "amazon":
        return add_ip_amazon(svc, s)
    elif rtype == "gce":
        return add_ip_gce(svc, s)

    kwargs = {}

    try:
        kwargs['ipname'] = svc.conf_get(s, 'ipname')
    except ex.OptNotFound as exc:
        kwargs['ipname'] = exc.default

    kwargs['ipdev'] = svc.conf_get(s, 'ipdev')

    try:
        kwargs['mask'] = svc.conf_get(s, 'netmask')
    except ex.OptNotFound as exc:
        kwargs['mask'] = exc.default

    try:
        kwargs['gateway'] = svc.conf_get(s, 'gateway')
    except ex.OptNotFound as exc:
        kwargs['gateway'] = exc.default

    try:
        zone = svc.conf_get(s, 'zone')
    except ex.OptNotFound as exc:
        zone = exc.default

    if rtype == "docker":
        try:
            kwargs['container_rid'] = svc.conf_get(s, 'container_rid')
        except ex.OptNotFound as exc:
            kwargs['container_rid'] = exc.default
        try:
            kwargs['network'] = svc.conf_get(s, 'network')
        except ex.OptNotFound as exc:
            kwargs['network'] = exc.default
        try:
            kwargs['del_net_route'] = svc.conf_get(s, 'del_net_route')
        except ex.OptNotFound as exc:
            kwargs['del_net_route'] = exc.default

    if rtype == "crossbow":
        try:
            kwargs['ipdevExt'] = svc.conf_get(s, 'ipdevext')
        except ex.OptNotFound as exc:
            kwargs['ipdevExt'] = exc.default
        if zone is not None:
            svc.log.error("'zone' and 'type=crossbow' are incompatible in section %s"%s)
            return
        ip = __import__('resIpCrossbow')
    elif zone is not None:
        kwargs['zone'] = zone
        ip = __import__('resIpZone')
    elif rtype == "docker":
        ip = __import__('resIpDocker'+rcEnv.sysname)
    else:
        ip = __import__('resIp'+rcEnv.sysname)

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    r = ip.Ip(**kwargs)
    svc += r

def add_md(svc, s):
    kwargs = {}

    kwargs['uuid'] = svc.conf_get(s, 'uuid')

    try:
        kwargs['shared'] = svc.conf_get(s, 'shared')
    except ex.OptNotFound:
        if len(svc.nodes|svc.drpnodes) < 2:
            kwargs['shared'] = False
            svc.log.debug("md %s shared param defaults to %s due to single node configuration"%(s, kwargs['shared']))
        else:
            l = [p for p in svc.config.options(s) if "@" in p]
            if len(l) > 0:
                kwargs['shared'] = False
                svc.log.debug("md %s shared param defaults to %s due to scoped configuration"%(s, kwargs['shared']))
            else:
                kwargs['shared'] = True
                svc.log.debug("md %s shared param defaults to %s due to unscoped configuration"%(s, kwargs['shared']))

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    m = __import__('resDiskMdLinux')
    r = m.Disk(**kwargs)
    svc += r

def add_drbd(svc, s):
    """Parse the configuration file and add a drbd object for each [drbd#n]
    section. Drbd objects are stored in a list in the service object.
    """
    kwargs = {}

    kwargs['res'] = svc.conf_get(s, 'res')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    mod = __import__('resDiskDrbd')
    r = mod.Drbd(**kwargs)
    svc += r

def add_vdisk(svc, s):
    kwargs = {}
    devpath = {}

    for attr, val in svc.config.items(s):
        if 'path@' in attr:
            devpath[attr.replace('path@', '')] = val

    if len(devpath) == 0:
        svc.log.error("path@node must be set in section %s"%s)
        return

    kwargs['devpath'] = devpath
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    m = __import__('resDiskVdisk')
    r = m.Disk(**kwargs)
    svc += r

def add_stonith(svc, s):
    if rcEnv.nodename in svc.drpnodes:
        # no stonith on DRP nodes
        return

    kwargs = {}

    _type = svc.conf_get(s, 'type')
    if len(_type) > 1:
        _type = _type[0].upper()+_type[1:].lower()

    if _type == 'Ilo':
        kwargs['name'] = svc.conf_get(s, 'target')
    elif _type == 'Callout':
        kwargs['cmd'] = svc.conf_get(s, 'cmd')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)

    st = __import__('resStonith'+_type)
    try:
        st = __import__('resStonith'+_type)
    except ImportError:
        svc.log.error("resStonith%s is not implemented"%_type)
        return

    r = st.Stonith(**kwargs)
    svc += r

def add_loop(svc, s):
    """Parse the configuration file and add a loop object for each [loop#n]
    section. Loop objects are stored in a list in the service object.
    """
    kwargs = {}

    kwargs['loopFile'] = svc.conf_get(s, 'file')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    try:
        m = __import__('resDiskLoop'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resDiskLoop%s is not implemented"%rcEnv.sysname)
        return

    r = m.Disk(**kwargs)
    svc += r


def add_disk_disk(svc, s):
    kwargs = {}
    try:
        kwargs['disk_id'] = svc.conf_get(s, 'disk_id')
    except ex.OptNotFound:
        pass

    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    m = __import__('resDiskDisk'+rcEnv.sysname)

    r = m.Disk(**kwargs)
    svc += r

def add_disk_gce(svc, s):
    kwargs = {}
    kwargs['names'] = svc.conf_get(s, 'names')
    kwargs['gce_zone'] = svc.conf_get(s, 'gce_zone')

    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    m = __import__('resDiskGce')

    r = m.Disk(**kwargs)
    svc += r

def add_disk_amazon(svc, s):
    kwargs = {}
    kwargs['volumes'] = svc.conf_get(s, 'volumes')

    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    m = __import__('resDiskAmazon')

    r = m.Disk(**kwargs)
    svc += r

def add_disk_rados(svc, s):
    kwargs = {}
    kwargs['images'] = svc.conf_get(s, 'images')
    try:
        kwargs['keyring'] = svc.conf_get(s, 'keyring')
    except ex.OptNotFound as exc:
        kwargs['keyring'] = exc.default
    try:
        kwargs['client_id'] = svc.conf_get(s, 'client_id')
    except ex.OptNotFound as exc:
        kwargs['client_id'] = exc.default
    try:
        lock_shared_tag = svc.conf_get(s, 'lock_shared_tag')
    except ex.OptNotFound as exc:
        lock_shared_tag = exc.default
    try:
        lock = svc.conf_get(s, 'lock')
    except ex.OptNotFound as exc:
        lock = exc.default

    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    try:
        m = __import__('resDiskRados'+rcEnv.sysname)
    except ImportError:
        svc.log.error("disk type rados is not implemented")
        return

    r = m.Disk(**kwargs)
    svc += r

    if not lock:
        return

    # rados locking resource
    kwargs["rid"] = kwargs["rid"]+"lock"
    kwargs["lock"] = lock
    kwargs["lock_shared_tag"] = lock_shared_tag
    r = m.DiskLock(**kwargs)
    svc += r


def add_raw(svc, s):
    kwargs = {}
    disk_type = "Raw"+rcEnv.sysname
    try:
        zone = svc.conf_get(s, 'zone')
    except ex.OptNotFound as exc:
        zone = exc.default

    kwargs['devs'] = svc.conf_get(s, 'devs')

    if zone is not None:
        kwargs['devs'] = set([dev.replace(":", ":<%s>" % zone) for dev in kwargs['devs']])

    try:
        kwargs['user'] = svc.conf_get(s, 'user')
    except ex.OptNotFound as exc:
        kwargs['user'] = exc.default
    try:
        kwargs['group'] = svc.conf_get(s, 'group')
    except ex.OptNotFound as exc:
        kwargs['group'] = exc.default
    try:
        kwargs['perm'] = svc.conf_get(s, 'perm')
    except ex.OptNotFound as exc:
        kwargs['perm'] = exc.default
    try:
        kwargs['create_char_devices'] = svc.conf_get(s, 'create_char_devices')
    except ex.OptNotFound as exc:
        kwargs['create_char_devices'] = exc.default

    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    try:
        m = __import__('resDisk'+disk_type)
    except ImportError:
        svc.log.error("disk type %s driver is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    if zone is not None:
        r.tags.add('zone')
        r.tags.add(zone)
    svc += r

def add_gandi(svc, s):
    disk_type = "Gandi"
    kwargs = {}
    kwargs['cloud_id'] = svc.conf_get(s, 'cloud_id')
    kwargs['name'] = svc.conf_get(s, 'name')

    try:
        kwargs['node'] = svc.conf_get(s, 'node')
    except ex.OptNotFound as exc:
        pass
    try:
        kwargs['user'] = svc.conf_get(s, 'user')
    except ex.OptNotFound as exc:
        pass
    try:
        kwargs['group'] = svc.conf_get(s, 'user')
    except ex.OptNotFound as exc:
        pass
    try:
        kwargs['perm'] = svc.conf_get(s, 'perm')
    except ex.OptNotFound as exc:
        pass

    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    try:
        m = __import__('resDisk'+disk_type)
    except ImportError:
        svc.log.error("disk type %s is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    svc += r

def add_disk_compat(svc, s):
    try:
        disk_type = svc.conf_get(s, 'type')
    except ex.OptNotFound as exc:
        disk_type = s.split("#")[0]
    if len(disk_type) >= 2:
        disk_type = disk_type[0].upper() + disk_type[1:].lower()

    if disk_type == 'Drbd':
        add_drbd(svc, s)
        return
    if disk_type == 'Vdisk':
        add_vdisk(svc, s)
        return
    if disk_type == 'Vmdg':
        add_vmdg(svc, s)
        return
    if disk_type == 'Pool':
        add_zpool(svc, s)
        return
    if disk_type == 'Zpool':
        add_zpool(svc, s)
        return
    if disk_type == 'Loop':
        add_loop(svc, s)
        return
    if disk_type == 'Md':
        add_md(svc, s)
        return
    if disk_type == 'Gce':
        add_disk_gce(svc, s)
        return
    if disk_type == 'Disk':
        add_disk_disk(svc, s)
        return
    if disk_type == 'Amazon':
        add_disk_amazon(svc, s)
        return
    if disk_type == 'Rados':
        add_disk_rados(svc, s)
        return
    if disk_type == 'Raw':
        add_raw(svc, s)
        return
    if disk_type == 'Gandi':
        add_gandi(svc, s)
        return
    if disk_type == 'Veritas':
        add_veritas(svc, s)
        return

    raise ex.OptNotFound

def add_veritas(svc, s):
    kwargs = {}
    kwargs['name'] = svc.conf_get(s, 'name')

    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    try:
        m = __import__('resDiskVgVeritas')
    except ImportError:
        svc.log.error("disk type veritas is not implemented")
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
    kwargs = {}
    kwargs['name'] = svc.conf_get(s, 'name')

    try:
        kwargs['dsf'] = svc.conf_get(s, 'dsf')
    except ex.OptNotFound as exc:
        pass
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    try:
        m = __import__('resDiskVg'+disk_type)
    except ImportError:
        svc.log.error("disk type %s is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    svc += r

def add_sync(svc, s):
    try:
        rtype = svc.conf_get(s, 'type')
    except ex.OptNotFound as exc:
        rtype = "rsync"
    globals()["add_sync_"+rtype](svc, s)

def add_container(svc, s):
    rtype = svc.conf_get(s, 'type')
    globals()["add_container_"+rtype](svc, s)

def add_disk(svc, s):
    """Parse the configuration file and add a disk object for each [disk#n]
    section. Disk objects are stored in a list in the service object.
    """
    try:
        disk_type = svc.conf_get(s, 'type')
    except ex.OptNotFound as exc:
        disk_type = s.split("#")[0]

    if len(disk_type) >= 2:
        disk_type = disk_type[0].upper() + disk_type[1:].lower()

    if disk_type == 'Drbd':
        add_drbd(svc, s)
        return
    if disk_type == 'Vdisk':
        add_vdisk(svc, s)
        return
    if disk_type == 'Vmdg':
        add_vmdg(svc, s)
        return
    if disk_type == 'Pool':
        add_zpool(svc, s)
        return
    if disk_type == 'Zpool':
        add_zpool(svc, s)
        return
    if disk_type == 'Loop':
        add_loop(svc, s)
        return
    if disk_type == 'Md':
        add_md(svc, s)
        return
    if disk_type == 'Gce':
        add_disk_gce(svc, s)
        return
    if disk_type == 'Disk':
        add_disk_disk(svc, s)
        return
    if disk_type == 'Amazon':
        add_disk_amazon(svc, s)
        return
    if disk_type == 'Rados':
        add_disk_rados(svc, s)
        return
    if disk_type == 'Raw':
        add_raw(svc, s)
        return
    if disk_type == 'Gandi':
        add_gandi(svc, s)
        return
    if disk_type == 'Veritas':
        add_veritas(svc, s)
        return
    if disk_type == 'Lvm' or disk_type == 'Vg' or disk_type == rcEnv.sysname:
        add_vg(svc, s)
        return

def add_vmdg(svc, s):
    kwargs = {}

    kwargs['container_id'] = svc.conf_get(s, 'container_id')

    if not svc.config.has_section(kwargs['container_id']):
        svc.log.error("%s.container_id points to an invalid section"%kwargs['container_id'])
        return

    try:
        container_type = svc.conf_get(kwargs['container_id'], 'type')
    except ex.OptNotFound as exc:
        svc.log.error("type must be set in section %s"%kwargs['container_id'])
        return

    if container_type == 'ldom':
        m = __import__('resDiskLdom')
    else:
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['name'] = s
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    r = m.Disk(**kwargs)
    svc += r

def add_zpool(svc, s):
    """Parse the configuration file and add a zpool object for each disk.zpool
    section. Pools objects are stored in a list in the service object.
    """
    kwargs = {}

    kwargs['name'] = svc.conf_get(s, 'name')

    try:
        zone = svc.conf_get(s, 'zone')
    except ex.OptNotFound as exc:
        zone = None

    m = __import__('resDiskZfs')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    r = m.Disk(**kwargs)

    if zone is not None:
        r.tags.add('zone')
        r.tags.add(zone)

    svc += r

def add_share(svc, s):
    _type = svc.conf_get(s, 'type')

    fname = 'add_share_'+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s))
    globals()[fname](svc, s)

def add_share_nfs(svc, s):
    kwargs = {}

    kwargs['path'] = svc.conf_get(s, 'path')
    kwargs['opts'] = svc.conf_get(s, 'opts')

    try:
        m = __import__('resShareNfs'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resShareNfs%s is not implemented"%rcEnv.sysname)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    r = m.Share(**kwargs)

    svc += r

def add_fs_directory(svc, s):
    kwargs = {}

    kwargs['path'] = svc.conf_get(s, 'path')

    try:
        kwargs['user'] = svc.conf_get(s, 'user')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['group'] = svc.conf_get(s, 'group')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['perm'] = svc.conf_get(s, 'perm')
    except ex.OptNotFound as exc:
        pass

    try:
        zone = svc.conf_get(s, 'zone')
    except:
        zone = None

    if zone is not None:
        zp = None
        for r in svc.get_resources("container.zone", discard_disabled=False):
            if r.name == zone:
                try:
                    zp = r.get_zonepath()
                except:
                    zp = "<%s>" % zone
                break
        if zp is None:
            svc.log.error("zone %s, referenced in %s, not found"%(zone, s))
            raise ex.excError()
        kwargs['path'] = zp+'/root'+kwargs['path']
        if "<%s>" % zone != zp:
            kwargs['path'] = os.path.realpath(kwargs['path'])

    mod = __import__('resFsDir')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    r = mod.FsDir(**kwargs)

    if zone is not None:
        r.tags.add(zone)
        r.tags.add('zone')

    svc += r

def add_fs(svc, s):
    """Parse the configuration file and add a fs object for each [fs#n]
    section. Fs objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['fs_type'] = svc.conf_get(s, 'type')
    except ex.OptNotFound as exc:
        kwargs['fs_type'] = ""

    if kwargs['fs_type'] == "directory":
        add_fs_directory(svc, s)
        return

    kwargs['device'] = svc.conf_get(s, 'dev')
    kwargs['mount_point'] = svc.conf_get(s, 'mnt')

    if kwargs['mount_point'][-1] != "/" and kwargs['mount_point'][-1] == '/':
        # Remove trailing / to not risk losing rsync src trailing / upon snap
        # mountpoint substitution.
        kwargs['mount_point'] = kwargs['mount_point'][0:-1]

    try:
        kwargs['mount_options'] = svc.conf_get(s, 'mnt_opt')
    except ex.OptNotFound as exc:
        kwargs['mount_options'] = ""

    try:
        kwargs['snap_size'] = svc.conf_get(s, 'snap_size')
    except ex.OptNotFound as exc:
        pass

    try:
        zone = svc.conf_get(s, 'zone')
    except:
        zone = None

    if zone is not None:
        zp = None
        for r in svc.get_resources("container.zone", discard_disabled=False):
            if r.name == zone:
                try:
                    zp = r.get_zonepath()
                except:
                    zp = "<%s>" % zone
                break
        if zp is None:
            svc.log.error("zone %s, referenced in %s, not found"%(zone, s))
            raise ex.excError()
        kwargs['mount_point'] = zp+'/root'+kwargs['mount_point']
        if "<%s>" % zone != zp:
            kwargs['mount_point'] = os.path.realpath(kwargs['mount_point'])

    try:
        mount = __import__('resFs'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resFs%s is not implemented"%rcEnv.sysname)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    r = mount.Mount(**kwargs)

    if zone is not None:
        r.tags.add(zone)
        r.tags.add('zone')

    svc += r

def add_container_esx(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerEsx')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Esx(**kwargs)
    svc += r

def add_container_hpvm(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerHpVm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.HpVm(**kwargs)
    svc += r

def add_container_ldom(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerLdom')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Ldom(**kwargs)
    svc += r

def add_container_vbox(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerVbox')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Vbox(**kwargs)
    svc += r

def add_container_xen(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerXen')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Xen(**kwargs)
    svc += r

def add_container_zone(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['delete_on_stop'] = svc.conf_get(s, 'delete_on_stop')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerZone')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Zone(**kwargs)
    svc += r



def add_container_vcloud(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    kwargs['cloud_id'] = svc.conf_get(s, 'cloud_id')
    kwargs['vapp'] = svc.conf_get(s, 'vapp')
    kwargs['key_name'] = svc.conf_get(s, 'key_name')

    m = __import__('resContainerVcloud')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.CloudVm(**kwargs)
    svc += r

def add_container_amazon(svc, s):
    kwargs = {}

    # mandatory keywords
    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    kwargs['cloud_id'] = svc.conf_get(s, 'cloud_id')
    kwargs['key_name'] = svc.conf_get(s, 'key_name')

    # optional keywords
    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    # provisioning keywords
    try:
        kwargs['image_id'] = svc.conf_get(s, 'image_id')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['size'] = svc.conf_get(s, 'size')
    except ex.OptNotFound as exc:
        pass


    try:
        kwargs['subnet'] = svc.conf_get(s, 'subnet')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerAmazon')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.CloudVm(**kwargs)
    svc += r

def add_container_openstack(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    kwargs['cloud_id'] = svc.conf_get(s, 'cloud_id')
    kwargs['key_name'] = svc.conf_get(s, 'key_name')

    try:
        kwargs['size'] = svc.conf_get(s, 'size')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['shared_ip_group'] = svc.conf_get(s, 'shared_ip_group')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerOpenstack')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.CloudVm(**kwargs)
    svc += r

def add_container_vz(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerVz')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Vz(**kwargs)
    svc += r

def add_container_kvm(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerKvm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Kvm(**kwargs)
    svc += r

def add_container_srp(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerSrp')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Srp(**kwargs)
    svc += r

def add_container_lxc(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['cf'] = svc.conf_get(s, 'cf')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerLxc')

    kwargs['rid'] = s
    kwargs['rcmd'] = get_rcmd(svc, s)
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Lxc(**kwargs)
    svc += r

def add_container_docker(svc, s):
    kwargs = {}

    kwargs['run_image'] = svc.conf_get(s, 'run_image')

    try:
        kwargs['run_command'] = svc.conf_get(s, 'run_command')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['run_args'] = svc.conf_get(s, 'run_args')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['docker_service'] = svc.conf_get(s, 'docker_service')
    except ex.OptNotFound as exc:
        kwargs['docker_service'] = exc.default

    m = __import__('resContainerDocker')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Docker(**kwargs)
    svc += r

def add_container_ovm(svc, s):
    kwargs = {}

    kwargs['uuid'] = svc.conf_get(s, 'uuid')

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get(s, 'guestos')
    except ex.OptNotFound as exc:
        pass

    m = __import__('resContainerOvm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Ovm(**kwargs)
    svc += r

def add_container_jail(svc, s):
    kwargs = {}
    kwargs['jailroot'] = svc.conf_get(s, 'jailroot')

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        kwargs['name'] = svc.svcname

    try:
        kwargs['ips'] = svc.conf_get(s, 'ips')
    except ex.OptNotFound as exc:
        kwargs['ips'] = exc.default

    try:
        kwargs['ip6s'] = svc.conf_get(s, 'ip6s')
    except ex.OptNotFound as exc:
        kwargs['ip6s'] = exc.default

    m = __import__('resContainerJail')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)
    kwargs['osvc_root_path'] = get_osvc_root_path(svc, s)

    r = m.Jail(**kwargs)
    svc += r

def add_mandatory_syncs(svc):
    """Mandatory files to sync:
    1/ to all nodes: service definition
    """

    def add_file(flist, fpath):
        if not os.path.exists(fpath):
            return flist
        flist.append(fpath)
        return flist

    if len(svc.nodes|svc.drpnodes) > 1:
        kwargs = {}
        src = []
        src = add_file(src, svc.paths.exe)
        src = add_file(src, svc.paths.cf)
        src = add_file(src, svc.paths.initd)
        src = add_file(src, svc.paths.alt_initd)
        dst = os.path.join("/")
        exclude = ['--exclude=*.core']
        kwargs['rid'] = "sync#i0"
        kwargs['src'] = src
        kwargs['dst'] = dst
        kwargs['options'] = ['-R']+exclude
        if svc.config.has_option(kwargs['rid'], 'options'):
            kwargs['options'] += cmdline2list(svc.config.get(kwargs['rid'], 'options'))
        kwargs['target'] = ["nodes", "drpnodes"]
        kwargs['internal'] = True
        kwargs['disabled'] = get_disabled(svc, kwargs['rid'])
        kwargs['optional'] = get_optional(svc, kwargs['rid'])
        kwargs.update(get_sync_args(svc, kwargs['rid']))
        r = resSyncRsync.Rsync(**kwargs)
        svc += r

def add_sync_docker(svc, s):
    kwargs = {}

    kwargs['target'] = svc.conf_get(s, 'target')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    m = __import__('resSyncDocker')
    r = m.SyncDocker(**kwargs)
    svc += r

def add_sync_btrfs(svc, s):
    kwargs = {}

    kwargs['src'] = svc.conf_get(s, 'src')
    kwargs['dst'] = svc.conf_get(s, 'dst')
    kwargs['target'] = svc.conf_get(s, 'target')

    try:
        kwargs['recursive'] = svc.conf_get(s, 'recursive')
    except ex.OptNotFound as exc:
        kwargs['recursive'] = exc.default

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    btrfs = __import__('resSyncBtrfs')
    r = btrfs.SyncBtrfs(**kwargs)
    svc += r

def add_sync_zfs(svc, s):
    kwargs = {}

    kwargs['src'] = svc.conf_get(s, 'src')
    kwargs['dst'] = svc.conf_get(s, 'dst')
    kwargs['target'] = svc.conf_get(s, 'target')

    try:
        kwargs['recursive'] = svc.conf_get(s, 'recursive')
    except ex.OptNotFound as exc:
        kwargs['recursive'] = exc.default

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    zfs = __import__('resSyncZfs')
    r = zfs.SyncZfs(**kwargs)
    svc += r

def add_sync_dds(svc, s):
    kwargs = {}

    kwargs['src'] = svc.conf_get(s, 'src')
    kwargs['target'] = svc.conf_get(s, 'target')

    dsts = {}
    for node in svc.nodes | svc.drpnodes:
        dst = svc.conf_get(s, 'dst', impersonate=node)
        dsts[node] = dst

    if len(dsts) == 0:
        for node in svc.nodes | svc.drpnodes:
            dsts[node] = kwargs['src']

    kwargs['dsts'] = dsts

    try:
        kwargs['snap_size'] = svc.conf_get(s, 'snap_size')
    except ex.OptNotFound as exc:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    dds = __import__('resSyncDds')
    r = dds.syncDds(**kwargs)
    svc += r

def add_sync_dcsckpt(svc, s):
    kwargs = {}

    kwargs['dcs'] = svc.conf_get(s, 'dcs')
    kwargs['manager'] = svc.conf_get(s, 'manager')
    raw_pairs = svc.conf_get(s, 'pairs')

    import json
    try:
        pairs = json.loads(raw_pairs)
        if len(pairs) == 0:
            svc.log.error("config file section %s must have 'pairs' set" % s)
            return
    except:
        svc.log.error("json error parsing 'pairs' in section %s" % s)
    kwargs['pairs'] = pairs

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncDcsCkpt'+rcEnv.sysname)
    except:
        sc = __import__('resSyncDcsCkpt')
    r = sc.syncDcsCkpt(**kwargs)
    svc += r

def add_sync_dcssnap(svc, s):
    kwargs = {}

    try:
        kwargs['dcs'] = svc.conf_get(s, 'dcs')
    except ex.OptNotFound as exc:
        svc.log.error("config file section %s must have 'dcs' set" % s)
        return

    try:
        kwargs['manager'] = svc.conf_get(s, 'manager')
    except ex.OptNotFound as exc:
        svc.log.error("config file section %s must have 'manager' set" % s)
        return

    try:
        kwargs['snapname'] = svc.conf_get(s, 'snapname')
    except ex.OptNotFound as exc:
        svc.log.error("config file section %s must have 'snapname' set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncDcsSnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncDcsSnap')
    r = sc.syncDcsSnap(**kwargs)
    svc += r

def add_sync_s3(svc, s):
    kwargs = {}

    try:
        kwargs['full_schedule'] = svc.conf_get(s, 'full_schedule')
    except ex.OptNotFound as exc:
        kwargs['full_schedule'] = exc.default

    try:
        kwargs['options'] = svc.conf_get(s, 'options')
    except ex.OptNotFound as exc:
        kwargs['options'] = exc.default

    try:
        kwargs['snar'] = svc.conf_get(s, 'snar')
    except ex.OptNotFound as exc:
        kwargs['snar'] = exc.default

    kwargs['bucket'] = svc.conf_get(s, 'bucket')
    kwargs['src'] = svc.conf_get(s, 'src')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__('resSyncS3')
    r = sc.syncS3(**kwargs)
    svc += r

def add_sync_zfssnap(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['keep'] = svc.conf_get(s, 'keep')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['recursive'] = svc.conf_get(s, 'recursive')
    except ex.OptNotFound as exc:
        pass

    kwargs['dataset'] = svc.conf_get(s, 'dataset')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__('resSyncZfsSnap')
    r = sc.syncZfsSnap(**kwargs)
    svc += r

def add_sync_btrfssnap(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get(s, 'name')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['keep'] = svc.conf_get(s, 'keep')
    except ex.OptNotFound as exc:
        pass

    kwargs['subvol'] = svc.conf_get(s, 'subvol')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__('resSyncBtrfsSnap')
    r = sc.syncBtrfsSnap(**kwargs)
    svc += r

def add_sync_necismsnap(svc, s):
    kwargs = {}

    try:
        kwargs['array'] = svc.conf_get(s, 'array')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['devs'] = svc.conf_get(s, 'devs')
    except ex.OptNotFound as exc:
        svc.log.error("config file section %s must have devs set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncNecIsmSnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncNecIsmSnap')
    r = sc.syncNecIsmSnap(**kwargs)
    svc += r

def add_sync_evasnap(svc, s):
    kwargs = {}

    try:
        kwargs['eva_name'] = svc.conf_get(s, 'eva_name')
    except ex.OptNotFound as exc:
        svc.log.error("config file section %s must have eva_name set" % s)
        return

    try:
        kwargs['snap_name'] = svc.conf_get(s, 'snap_name')
    except ex.OptNotFound as exc:
        kwargs['snap_name'] = svc.svcname

    import json
    pairs = []
    if 'pairs' in svc.config.options(s):
        pairs = json.loads(svc.config.get(s, 'pairs'))
    if len(pairs) == 0:
        svc.log.error("config file section %s must have pairs set" % s)
        return
    else:
        kwargs['pairs'] = pairs

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncEvasnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncEvasnap')
    r = sc.syncEvasnap(**kwargs)
    svc += r

def add_sync_hp3parsnap(svc, s):
    kwargs = {}

    kwargs['array'] = svc.conf_get(s, 'array')
    vv_names = svc.conf_get(s, 'vv_names')

    if len(vv_names) == 0:
        svc.log.error("config file section %s must have at least one vv_name set" % s)
        return

    kwargs['vv_names'] = vv_names

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncHp3parSnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncHp3parSnap')
    r = sc.syncHp3parSnap(**kwargs)
    svc += r

def add_sync_hp3par(svc, s):
    kwargs = {}

    kwargs['mode'] = svc.conf_get(s, 'mode')
    kwargs['array'] = svc.conf_get(s, 'array')

    rcg_names = {}
    for node in svc.nodes | svc.drpnodes:
        array = svc.conf_get(s, 'array', impersonate=node)
        rcg = svc.conf_get(s, 'rcg', impersonate=node)
        rcg_names[array] = rcg

    if len(rcg_names) == 0:
        svc.log.error("config file section %s must have rcg set" % s)
        return

    kwargs['rcg_names'] = rcg_names

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncHp3par'+rcEnv.sysname)
    except:
        sc = __import__('resSyncHp3par')
    r = sc.syncHp3par(**kwargs)
    svc += r

def add_sync_symsrdfs(svc, s):
    kwargs = {}

    kwargs['symdg'] = svc.conf_get(s, 'symdg')
    kwargs['rdfg'] = svc.conf_get(s, 'rdfg')
    kwargs['symid'] = svc.conf_get(s, 'symid')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncSymSrdfS'+rcEnv.sysname)
    except:
        sc = __import__('resSyncSymSrdfS')
    r = sc.syncSymSrdfS(**kwargs)
    svc += r


def add_sync_radosclone(svc, s):
    kwargs = {}

    try:
        kwargs['client_id'] = svc.conf_get(s, 'client_id')
    except ex.OptNotFound as exc:
        pass

    try:
        kwargs['keyring'] = svc.conf_get(s, 'keyring')
    except ex.OptNotFound as exc:
        pass

    kwargs['pairs'] = svc.conf_get(s, 'pairs')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncRados'+rcEnv.sysname)
    except:
        sc = __import__('resSyncRados')
    r = sc.syncRadosClone(**kwargs)
    svc += r

def add_sync_radossnap(svc, s):
    kwargs = {}

    try:
        kwargs['client_id'] = svc.conf_get(s, 'client_id')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keyring'] = svc.conf_get(s, 'keyring')
    except ex.OptNotFound:
        pass

    kwargs['images'] = svc.conf_get(s, 'images')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncRados'+rcEnv.sysname)
    except:
        sc = __import__('resSyncRados')
    r = sc.syncRadosSnap(**kwargs)
    svc += r

def add_sync_symsnap(svc, s):
    _add_sync_symclone(svc, s, "sync.symsnap")

def add_sync_symclone(svc, s):
    _add_sync_symclone(svc, s, "sync.symclone")

def _add_sync_symclone(svc, s, t):
    kwargs = {}
    kwargs['type'] = t
    kwargs['pairs'] = svc.conf_get(s, 'pairs')
    kwargs['symid'] = svc.conf_get(s, 'symid')

    try:
        kwargs['recreate_timeout'] = svc.conf_get(s, 'recreate_timeout')
    except ex.OptNotFound as exc:
        kwargs['recreate_timeout'] = exc.default

    try:
        kwargs['restore_timeout'] = svc.conf_get(s, 'restore_timeout')
    except ex.OptNotFound as exc:
        kwargs['restore_timeout'] = exc.default

    try:
        kwargs['consistent'] = svc.conf_get(s, 'consistent')
    except ex.OptNotFound as exc:
        kwargs['consistent'] = exc.default

    try:
        kwargs['precopy'] = svc.conf_get(s, 'precopy')
    except ex.OptNotFound as exc:
        kwargs['precopy'] = exc.default

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        sc = __import__('resSyncSymclone'+rcEnv.sysname)
    except:
        sc = __import__('resSyncSymclone')
    r = sc.syncSymclone(**kwargs)
    svc += r

def add_sync_ibmdssnap(svc, s):
    kwargs = {}

    kwargs['pairs'] = svc.conf_get(s, 'pairs')
    kwargs['array'] = svc.conf_get(s, 'array')
    kwargs['bgcopy'] = svc.conf_get(s, 'bgcopy')
    kwargs['recording'] = svc.conf_get(s, 'recording')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    try:
        m = __import__('resSyncIbmdsSnap'+rcEnv.sysname)
    except:
        m = __import__('resSyncIbmdsSnap')
    r = m.syncIbmdsSnap(**kwargs)
    svc += r

def add_sync_nexenta(svc, s):
    kwargs = {}

    kwargs['name'] = svc.conf_get(s, 'name')
    kwargs['path'] = svc.conf_get(s, 'path')
    kwargs['reversible'] = svc.conf_get(s, "reversible")

    filers = {}
    for n in svc.nodes | svc.drpnodes:
        filers[n] = svc.conf_get(s, 'filer', impersonate=n)

    kwargs['filers'] = filers
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    import resSyncNexenta
    r = resSyncNexenta.syncNexenta(**kwargs)
    svc += r

def add_sync_netapp(svc, s):
    kwargs = {}

    kwargs['path'] = svc.conf_get(s, 'path')
    kwargs['user'] = svc.conf_get(s, 'user')

    filers = {}
    for n in svc.nodes | svc.drpnodes:
        filers[n] = svc.conf_get(s, 'filer', impersonate=n)

    kwargs['filers'] = filers
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    import resSyncNetapp
    r = resSyncNetapp.syncNetapp(**kwargs)
    svc += r

def add_sync_rsync(svc, s):
    if s.startswith("sync#i"):
        # internal syncs have their own dedicated add function
        return

    kwargs = {}
    kwargs['src'] = []
    _s = svc.conf_get(s, 'src')
    for src in _s:
        kwargs['src'] += glob.glob(src)

    kwargs['dst'] = svc.conf_get(s, 'dst')

    try:
        kwargs['options'] = svc.conf_get(s, 'options')
    except ex.OptNotFound as exc:
        kwargs['options'] = exc.default

    try:
        kwargs['dstfs'] = svc.conf_get(s, 'dstfs')
    except ex.OptNotFound as exc:
        kwargs['dstfs'] = exc.default

    try:
        kwargs['snap'] = svc.conf_get(s, 'snap')
    except ex.OptNotFound as exc:
        kwargs['snap'] = exc.default

    try:
        kwargs['bwlimit'] = svc.conf_get(s, 'bwlimit')
    except ex.OptNotFound as exc:
        pass

    kwargs['target'] = svc.conf_get(s, 'target')
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    r = resSyncRsync.Rsync(**kwargs)
    svc += r

def add_task(svc, s):
    kwargs = {}

    kwargs['command'] = svc.conf_get(s, 'command')

    try:
        kwargs['on_error'] = svc.conf_get(s, 'on_error')
    except ex.OptNotFound as exc:
        kwargs['on_error'] = exc.default

    try:
        kwargs['user'] = svc.conf_get(s, 'user')
    except ex.OptNotFound as exc:
        kwargs['user'] = exc.default

    try:
        kwargs['confirmation'] = svc.conf_get(s, 'confirmation')
    except ex.OptNotFound as exc:
        kwargs['confirmation'] = exc.default

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    import resTask
    r = resTask.Task(**kwargs)
    svc += r

def add_app(svc, s):
    resApp = ximport('resApp')
    kwargs = {}

    kwargs['script'] = svc.conf_get(s, 'script')

    try:
        kwargs['start'] = svc.conf_get(s, 'start')
    except ex.OptNotFound as exc:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'start'))
        return

    try:
        kwargs['stop'] = svc.conf_get(s, 'stop')
    except ex.OptNotFound as exc:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'stop'))
        return

    try:
        kwargs['check'] = svc.conf_get(s, 'check')
    except ex.OptNotFound as exc:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'check'))
        return

    try:
        kwargs['info'] = svc.conf_get(s, 'info')
    except ex.OptNotFound as exc:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'info'))
        return

    try:
        kwargs['timeout'] = svc.conf_get(s, 'timeout')
    except ex.OptNotFound as exc:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['always_on'] = always_on_nodes_set(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs['monitor'] = get_monitor(svc, s)
    kwargs['restart'] = get_restart(svc, s)

    r = resApp.App(**kwargs)
    svc += r


def setup_logging(svcnames):
    """Setup logging to stream + logfile, and logfile rotation
    class Logger instance name: 'log'
    """
    max_svcname_len = 0

    # compute max svcname length to align logging stream output
    for svcname in svcnames:
        n = len(svcname)
        if n > max_svcname_len:
            max_svcname_len = n

    rcLogger.max_svcname_len = max_svcname_len
    rcLogger.initLogger(rcEnv.nodename)

def build(name, minimal=False, svcconf=None):
    """build(name) is in charge of Svc creation
    it return None if service Name is not managed by local node
    else it return new Svc instance
    """
    import svc

    #
    # node discovery is hidden in a separate module to
    # keep it separate from the framework stuff
    #
    discover_node()
    svc = svc.Svc(svcname=name)

    try:
        encapnodes = svc.conf_get('DEFAULT', "encapnodes")
    except ex.OptNotFound as exc:
        encapnodes = exc.default
    svc.encapnodes = set(encapnodes)

    try:
        nodes = svc.conf_get('DEFAULT', "nodes")
    except ex.OptNotFound as exc:
        nodes = exc.default
    svc.ordered_nodes = nodes
    svc.nodes = set(nodes)

    try:
        drpnodes = svc.conf_get('DEFAULT', "drpnodes")
    except ex.OptNotFound as exc:
        drpnodes = exc.default

    try:
        drpnode = svc.conf_get('DEFAULT', "drpnode").lower()
        if drpnode not in drpnodes and drpnode != "":
            drpnodes.append(drpnode)
    except ex.OptNotFound as exc:
        drpnode = ''
    svc.ordered_drpnodes = drpnodes
    svc.drpnodes = set(drpnodes)

    try:
        flex_primary = svc.conf_get('DEFAULT', "flex_primary").lower()
    except ex.OptNotFound as exc:
        if len(nodes) > 0:
            flex_primary = nodes[0]
        else:
            flex_primary = ''
    svc.flex_primary = flex_primary

    try:
        drp_flex_primary = svc.conf_get('DEFAULT', "drp_flex_primary").lower()
    except ex.OptNotFound as exc:
        if len(drpnodes) > 0:
            drp_flex_primary = drpnodes[0]
        else:
            drp_flex_primary = ''
    svc.drp_flex_primary = drp_flex_primary

    try:
        svc.placement = svc.conf_get('DEFAULT', "placement")
    except ex.OptNotFound as exc:
        pass


    #
    # Store and validate the service type
    #
    if svc.conf_has_option_scoped("DEFAULT", "env"):
        svc.svc_env = svc.conf_get('DEFAULT', "env")
    elif svc.conf_has_option_scoped("DEFAULT", "service_type"):
        svc.svc_env = svc.conf_get('DEFAULT', "service_type")

    try:
        svc.lock_timeout = svc.conf_get('DEFAULT', 'lock_timeout')
    except ex.OptNotFound as exc:
        pass

    try:
        svc.presnap_trigger = svc.conf_get('DEFAULT', 'presnap_trigger')
    except ex.OptNotFound as exc:
        pass

    try:
        svc.postsnap_trigger = svc.conf_get('DEFAULT', 'postsnap_trigger')
    except ex.OptNotFound as exc:
        pass

    try:
        svc.disable_rollback = not svc.conf_get('DEFAULT', "rollback")
    except ex.OptNotFound as exc:
        pass

    svc.encap = rcEnv.nodename in svc.encapnodes

    #
    # amazon options
    #
    try:
        svc.aws = svc.conf_get("DEFAULT", 'aws')
    except ex.OptNotFound as exc:
        pass

    try:
        svc.aws_profile = svc.conf_get("DEFAULT", 'aws_profile')
    except ex.OptNotFound as exc:
        pass

    #
    # process group options
    #
    try:
        svc.create_pg = svc.conf_get("DEFAULT", 'create_pg')
    except ex.OptNotFound as exc:
        pass

    try:
        svc.affinity = svc.conf_get('DEFAULT', 'affinity')
    except ex.OptNotFound as exc:
        pass

    try:
        svc.anti_affinity = svc.conf_get('DEFAULT', 'anti_affinity')
    except ex.OptNotFound as exc:
        pass

    try:
        svc.clustertype = svc.conf_get('DEFAULT', 'cluster_type')
    except ex.OptNotFound as exc:
        pass

    if 'flex' in svc.clustertype:
        svc.ha = True

    try:
        svc.show_disabled = svc.conf_get('DEFAULT', 'show_disabled')
    except ex.OptNotFound as exc:
        svc.show_disabled = True

    # prune service whose service type does not match host mode
    if svc.svc_env != 'PRD' and rcEnv.node_env == 'PRD':
        raise ex.excInitError('not allowed to run on this node (svc env=%s node env=%s)' % (svc.svc_env, rcEnv.node_env))

    try:
        svc.comment = svc.conf_get('DEFAULT', 'comment')
    except ex.OptNotFound as exc:
        pass

    try:
        svc.monitor_action = svc.conf_get('DEFAULT', "monitor_action")
    except ex.OptNotFound as exc:
        pass

    try:
        svc.pre_monitor_action = svc.conf_get('DEFAULT', "pre_monitor_action")
    except ex.OptNotFound as exc:
        pass

    try:
        svc.bwlimit = svc.conf_get('DEFAULT', "bwlimit")
    except ex.OptNotFound as exc:
        svc.bwlimit = None

    try:
        svc.clustername = svc.conf_get('DEFAULT', "cluster")
    except ex.OptNotFound as exc:
        pass

    if minimal:
        svc.options.minimal = True
        return svc

    svc.options.minimal = False

    #
    # instanciate resources
    #
    add_resources(svc, 'container')
    add_resources(svc, 'stonith')
    add_resources(svc, 'ip')
    add_resources(svc, 'disk')
    add_resources(svc, 'fs')
    add_resources(svc, 'share')
    add_resources(svc, 'app')
    add_resources(svc, 'task')

    # deprecated, folded into "disk"
    add_resources(svc, 'vdisk')
    add_resources(svc, 'vmdg')
    add_resources(svc, 'loop')
    add_resources(svc, 'drbd')
    add_resources(svc, 'vg')
    add_resources(svc, 'pool')

    add_resources(svc, 'sync')
    add_mandatory_syncs(svc)

    svc.post_build()
    return svc

def is_service(f):
    if os.name == 'nt':
        return True
    if os.path.realpath(f) != os.path.realpath(rcEnv.paths.svcmgr):
        return False
    if not os.path.exists(f + '.conf'):
        return False
    return True

def list_services():
    if not os.path.exists(rcEnv.paths.pathetc):
        print("create dir %s"%rcEnv.paths.pathetc)
        os.makedirs(rcEnv.paths.pathetc)

    s = glob.glob(os.path.join(rcEnv.paths.pathetc, '*.conf'))
    s = [os.path.basename(x)[:-5] for x in s]

    l = []
    for name in s:
        if len(s) == 0:
            continue
        if not is_service(os.path.join(rcEnv.paths.pathetc, name)):
            continue
        l.append(name)
    return l

def build_services(status=None, svcnames=None, create_instance=False,
                   minimal=False):
    """
    Returns a list of all services of status matching the specified status.
    If no status is specified, returns all services.
    """
    import svc

    if svcnames is None:
        svcnames = []

    check_privs()

    errors = []
    services = {}

    if isinstance(svcnames, str):
        svcnames = [svcnames]

    if len(svcnames) == 0:
        svcnames = list_services()
        missing_svcnames = []
    else:
        all_svcnames = list_services()
        missing_svcnames = sorted(list(set(svcnames) - set(all_svcnames)))
        for m in missing_svcnames:
            if create_instance:
                services[m] = svc.Svc(m)
            else:
                errors.append("%s: service does not exist" % m)
        svcnames = list(set(svcnames) & set(all_svcnames))

    setup_logging(svcnames)

    for name in svcnames:
        try:
            svc = build(name, minimal=minimal)
        except (ex.excError, ex.excInitError, ValueError) as e:
            errors.append("%s: %s" % (name, str(e)))
            svclog = rcLogger.initLogger(rcEnv.nodename+"."+name, handlers=["file", "syslog"])
            svclog.error(str(e))
            continue
        except ex.excAbortAction:
            continue
        except:
            import traceback
            traceback.print_exc()
            continue
        if status is not None and not svc.status() in status:
            continue
        services[svc.svcname] = svc
    return [s for _, s in sorted(services.items())], errors

def create(svcname, resources=[], interactive=False, provision=False):
    if not isinstance(svcname, list):
        print("ouch, svcname should be a list object", file=sys.stderr)
        return {"ret": 1}
    if len(svcname) != 1:
        print("you must specify a single service name with the 'create' action", file=sys.stderr)
        return {"ret": 1}
    svcname = svcname[0]
    if len(svcname) == 0:
        print("service name must not be empty", file=sys.stderr)
        return {"ret": 1}
    if svcname in list_services():
        print("service", svcname, "already exists", file=sys.stderr)
        return {"ret": 1}
    cf = os.path.join(rcEnv.paths.pathetc, svcname+'.conf')
    if os.path.exists(cf):
        import shutil
        print(cf, "already exists. save as "+svcname+".conf.bak", file=sys.stderr)
        shutil.move(cf, os.path.join(rcEnv.paths.pathtmp, svcname+".conf.bak"))
    try:
        f = open(cf, 'w')
    except:
        print("failed to open", cf, "for writing", file=sys.stderr)
        return {"ret": 1}

    defaults = {}
    sections = {}
    rtypes = {}

    import json
    for r in resources:
        try:
            d = json.loads(r)
        except:
            print("can not parse resource:", r, file=sys.stderr)
            return {"ret": 1}
        if 'rid' in d:
            section = d['rid']
            if '#' not in section:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return {"ret": 1}
            l = section.split('#')
            if len(l) != 2:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return {"ret": 1}
            rtype = l[1]
            if rtype in rtypes:
                rtypes[rtype] += 1
            else:
                rtypes[rtype] = 0
            del d['rid']
            if section in sections:
                sections[section].update(d)
            else:
                sections[section] = d
        elif 'rtype' in d and d["rtype"] == "env":
            del d["rtype"]
            if "env" in sections:
                sections["env"].update(d)
            else:
                sections["env"] = d
        elif 'rtype' in d and d["rtype"] != "DEFAULT":
            if 'rid' in d:
                del d['rid']
            rtype = d['rtype']
            if rtype in rtypes:
                section = '%s#%d'%(rtype, rtypes[rtype])
                rtypes[rtype] += 1
            else:
                section = '%s#0'%rtype
                rtypes[rtype] = 1
            if section in sections:
                sections[section].update(d)
            else:
                sections[section] = d
        else:
            if "rtype" in d:
                del d["rtype"]
            defaults.update(d)

    from svcdict import KeyDict, MissKeyNoDefault, KeyInvalidValue
    try:
        keys = KeyDict(provision=provision)
        defaults.update(keys.update('DEFAULT', defaults))
        for section, d in sections.items():
            sections[section].update(keys.update(section, d))
    except (MissKeyNoDefault, KeyInvalidValue):
        if not interactive:
            return {"ret": 1}

    try:
        if interactive:
            defaults, sections = keys.form(defaults, sections)
    except KeyboardInterrupt:
        sys.stderr.write("Abort\n")
        return {"ret": 1}

    conf = rcConfigParser.RawConfigParser(defaults)
    for section, d in sections.items():
        conf.add_section(section)
        for key, val in d.items():
            if key == 'rtype':
                continue
            conf.set(section, key, val)

    conf.write(f)

    initdir = svcname+'.dir'
    svcinitdir = os.path.join(rcEnv.paths.pathetc, initdir)
    if not os.path.exists(svcinitdir):
        os.makedirs(svcinitdir)
    fix_app_link(svcname)
    fix_exe_link(rcEnv.paths.svcmgr, svcname)
    return {"ret": 0, "rid": sections.keys()}

def fix_app_link(svcname):
    os.chdir(rcEnv.paths.pathetc)
    src = svcname+'.d'
    dst = svcname+'.dir'
    if os.name != 'posix':
        return
    try:
        os.readlink(src)
    except:
        if not os.path.exists(dst):
            os.makedirs(dst)
        os.symlink(dst, src)

def fix_exe_link(dst, src):
    if os.name != 'posix':
        return
    os.chdir(rcEnv.paths.pathetc)
    try:
        p = os.readlink(src)
    except:
        os.symlink(dst, src)
        p = dst
    if p != dst:
        os.unlink(src)
        os.symlink(dst, src)
