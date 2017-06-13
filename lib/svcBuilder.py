from __future__ import print_function
import os
import sys
import logging
import re
import socket
import glob
import platform

from rcGlobalEnv import rcEnv, Storage
from rcNode import discover_node
import rcLogger
import resSyncRsync
import rcExceptions as ex
import rcConfigParser
from svc import Svc
from rcUtilities import convert_size, cmdline2list, ximport, \
                        check_privs

if 'PATH' not in os.environ:
    os.environ['PATH'] = ""
os.environ['LANG'] = 'C'
os.environ['PATH'] += ':/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'

def get_tags(svc, section):
    try:
        s = svc.conf_get_string_scope(section, 'tags')
    except ex.OptNotFound:
        s = ""
    return set(s.split())

def get_optional(svc, section):
    if not svc.config.has_section(section):
        try:
            return svc.conf_get_boolean_scope("DEFAULT", "optional")
        except:
            return False

    # deprecated
    if svc.config.has_option(section, 'optional_on'):
        nodes = set([])
        l = svc.config.get(section, "optional_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i.lower()])
        if rcEnv.nodename in nodes:
            return True
        return False

    try:
        return svc.conf_get_boolean_scope(section, "optional")
    except:
        return False

def get_monitor(svc, section):
    if not svc.config.has_section(section):
        try:
            return svc.conf_get_boolean_scope("DEFAULT", "monitor")
        except:
            return False

    # deprecated
    if svc.config.has_option(section, 'monitor_on'):
        nodes = set([])
        l = svc.config.get(section, "monitor_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i.lower()])
        if rcEnv.nodename in nodes:
            return True
        return False

    try:
        return svc.conf_get_boolean_scope(section, "monitor")
    except:
        return False

def get_rcmd(svc, section):
    if not svc.config.has_section(section):
        return
    try:
        return svc.conf_get_string_scope(section, 'rcmd').split()
    except ex.OptNotFound:
        return

def get_subset(svc, section):
    if not svc.config.has_section(section):
        return
    try:
        return svc.conf_get_string_scope(section, 'subset')
    except ex.OptNotFound:
        return
    return

def get_osvc_root_path(svc, section):
    if not svc.config.has_section(section):
        return
    try:
        return svc.conf_get_string_scope(section, 'osvc_root_path')
    except ex.OptNotFound:
        return
    return

def get_restart(svc, section):
    if not svc.config.has_section(section):
        if svc.config.has_option('DEFAULT', 'restart'):
            try:
                return svc.conf_get_int_scope(section, 'restart')
            except ex.OptNotFound:
                return 0
        else:
            return 0
    try:
        return svc.conf_get_int_scope(section, 'restart')
    except ex.OptNotFound:
        return 0
    return 0

def get_disabled(svc, section):
    # service-level disable takes precedence over all resource-level disable method
    if svc.config.has_option('DEFAULT', 'disable'):
        svc_disable = svc.config.getboolean("DEFAULT", "disable")
    else:
        svc_disable = False

    if svc_disable is True:
        return True

    if section == "":
        return svc_disable

    # unscopable enable_on option (takes precedence over disable and disable_on)
    nodes = set([])
    if svc.config.has_option(section, 'enable_on'):
        l = svc.conf_get_string_scope(section, "enable_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i.lower()])
        if rcEnv.nodename in nodes:
            return False

    # scoped disable option
    try:
        r = svc.conf_get_boolean_scope(section, 'disable')
    except ex.OptNotFound:
        r = False
    except Exception as e:
        print(e, "... consider section as disabled")
        r = True
    if r:
        return r

    # unscopable disable_on option
    nodes = set([])
    if svc.config.has_option(section, 'disable_on'):
        l = svc.config.get(section, "disable_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i.lower()])
    if rcEnv.nodename in nodes:
        return True

    return False

def need_scsireserv(svc, section):
    """scsireserv = true can be set globally or in a specific
    resource section
    """
    r = False
    try:
        r = svc.conf_get_boolean_scope(section, 'scsireserv')
    except ex.OptNotFound:
        defaults = svc.config.defaults()
        if 'scsireserv' in defaults:
            r = bool(defaults['scsireserv'])
    return r

def add_scsireserv(svc, resource, section):
    if not need_scsireserv(svc, section):
        return
    try:
        sr = __import__('resScsiReserv'+rcEnv.sysname)
    except ImportError:
        sr = __import__('resScsiReserv')

    kwargs = {}
    pr_rid = resource.rid+"pr"

    try:
        kwargs["prkey"] = svc.conf_get_string_scope(resource.rid, 'prkey')
    except ex.OptNotFound:
        pass

    try:
        pa = svc.conf_get_boolean_scope(resource.rid, 'no_preempt_abort')
    except ex.OptNotFound:
        pa = False

    try:
        kwargs['optional'] = get_optional(svc, pr_rid)
    except ex.OptNotFound:
        kwargs['optional'] = resource.is_optional()

    try:
        kwargs['disabled'] = get_disabled(svc, pr_rid)
    except ex.OptNotFound:
        kwargs['disabled'] = resource.is_disabled()

    try:
        kwargs['restart'] = get_restart(svc, pr_rid)
    except ex.OptNotFound:
        kwargs['restart'] = resource.restart

    try:
        kwargs['monitor'] = get_monitor(svc, pr_rid)
    except ex.OptNotFound:
        kwargs['monitor'] = resource.monitor

    try:
        kwargs['tags'] = get_tags(svc, pr_rid)
    except:
        kwargs['tags'] = set([])

    kwargs['rid'] = resource.rid
    kwargs['tags'] |= resource.tags
    kwargs['peer_resource'] = resource
    kwargs['no_preempt_abort'] = pa

    r = sr.ScsiReserv(**kwargs)
    svc += r

def add_triggers(svc, resource, section):
    triggers = [
      'pre_unprovision', 'post_unprovision',
      'pre_provision', 'post_provision',
      'pre_stop', 'pre_start',
      'post_stop', 'post_start',
      'pre_sync_nodes', 'pre_sync_drp',
      'post_sync_nodes', 'post_sync_drp',
      'post_sync_resync', 'pre_sync_resync',
      'post_sync_update', 'pre_sync_update',
      'post_run', 'pre_run',
    ]
    compat_triggers = [
      'pre_syncnodes', 'pre_syncdrp',
      'post_syncnodes', 'post_syncdrp',
      'post_syncresync', 'pre_syncresync',
      'post_syncupdate', 'pre_syncupdate',
    ]
    for trigger in triggers + compat_triggers:
        for prefix in ("", "blocking_"):
            try:
                s = svc.conf_get_string_scope(resource.rid, prefix+trigger)
            except ex.OptNotFound:
                continue
            if trigger in compat_triggers:
                trigger = trigger.replace("sync", "sync_")
            setattr(resource, prefix+trigger, s)

def add_requires(svc, resource, section):
    actions = [
      'unprovision', 'provision'
      'stop', 'start',
      'sync_nodes', 'sync_drp', 'sync_resync', 'sync_break', 'sync_update',
      'run',
    ]
    for action in actions:
        try:
            s = svc.conf_get_string_scope(section, action+'_requires')
        except ex.OptNotFound:
            continue
        s = s.replace("stdby ", "stdby_")
        l = s.split(" ")
        l = list(map(lambda x: x.replace("stdby_", "stdby "), l))
        setattr(resource, action+'_requires', l)

def add_triggers_and_requires(svc, resource, section):
    add_triggers(svc, resource, section)
    add_requires(svc, resource, section)

def always_on_nodes_set(svc, section):
    try:
        always_on_opt = svc.config.get(section, "always_on").split()
    except:
        always_on_opt = []
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
        kwargs['sync_max_delay'] = svc.conf_get_int_scope(s, 'sync_max_delay')
    elif 'sync_max_delay' in defaults:
        kwargs['sync_max_delay'] = svc.conf_get_int_scope('DEFAULT', 'sync_max_delay')

    if svc.config.has_option(s, 'schedule'):
        kwargs['schedule'] = svc.conf_get_string_scope(s, 'schedule')
    elif svc.config.has_option(s, 'period') or svc.config.has_option(s, 'sync_period'):
        # old schedule syntax compatibility
        from rcScheduler import Scheduler
        kwargs['schedule'] = Scheduler().sched_convert_to_schedule(svc.config, s, prefix='sync_')
    elif 'sync_schedule' in defaults:
        kwargs['schedule'] = svc.conf_get_string_scope('DEFAULT', 'sync_schedule')
    elif 'sync_period' in defaults:
        # old schedule syntax compatibility for internal sync
        from rcScheduler import Scheduler
        kwargs['schedule'] = Scheduler().sched_convert_to_schedule(svc.config, s, prefix='sync_')

    return kwargs

def add_resources(svc, restype):
    for s in svc.config.sections():
        add_resource(svc, restype, s)

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
            subset = svc.conf_get_string_scope(s, 'subset')
        except ex.OptNotFound:
            subset = None
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
        rtype = svc.conf_get_string_scope(s, 'type')
    except ex.OptNotFound:
        rtype = None

    if rtype != "gce":
        return

    try:
        kwargs['ipname'] = svc.conf_get_string_scope(s, 'ipname')
    except ex.OptNotFound:
        svc.log.error("ipname must be defined in config file section %s" % s)
        return

    try:
        kwargs['ipdev'] = svc.conf_get_string_scope(s, 'ipdev')
    except ex.OptNotFound:
        svc.log.error("ipdev must be defined in config file section %s" % s)
        return

    try:
        kwargs['eip'] = svc.conf_get_string_scope(s, 'eip')
    except ex.OptNotFound:
        pass

    try:
        kwargs['routename'] = svc.conf_get_string_scope(s, 'routename')
    except ex.OptNotFound:
        pass

    try:
        kwargs['gce_zone'] = svc.conf_get_string_scope(s, 'gce_zone')
    except ex.OptNotFound:
        pass

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_ip_amazon(svc, s):
    kwargs = {}

    try:
        rtype = svc.conf_get_string_scope(s, 'type')
    except ex.OptNotFound:
        rtype = None

    if rtype != "amazon":
        return

    try:
        kwargs['ipname'] = svc.conf_get_string_scope(s, 'ipname')
    except ex.OptNotFound:
        svc.log.error("nor ipname and ipname@%s defined in config file section %s"%(rcEnv.nodename, s))
        return

    try:
        kwargs['ipdev'] = svc.conf_get_string_scope(s, 'ipdev')
    except ex.OptNotFound:
        svc.log.error("ipdev must be defined in config file section %s" % s)
        return

    try:
        kwargs['eip'] = svc.conf_get_string_scope(s, 'eip')
    except ex.OptNotFound:
        pass

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_ip(svc, s):
    """Parse the configuration file and add an ip object for each [ip#n]
    section. Ip objects are stored in a list in the service object.
    """
    try:
        rtype = svc.conf_get_string_scope(s, 'type')
    except ex.OptNotFound:
        rtype = None

    if rtype == "amazon":
        return add_ip_amazon(svc, s)
    elif rtype == "gce":
        return add_ip_gce(svc, s)

    kwargs = {}

    try:
        kwargs['ipname'] = svc.conf_get_string_scope(s, 'ipname')
    except ex.OptNotFound:
        pass

    try:
        kwargs['ipdev'] = svc.conf_get_string_scope(s, 'ipdev')
    except ex.OptNotFound:
        svc.log.error('ipdev not found in ip section %s'%s)
        return

    try:
        kwargs['ipdevExt'] = svc.conf_get_string_scope(s, 'ipdevext')
    except ex.OptNotFound:
        pass

    try:
        kwargs['mask'] = svc.conf_get_string_scope(s, 'netmask')
    except ex.OptNotFound:
        pass

    try:
        kwargs['gateway'] = svc.conf_get_string_scope(s, 'gateway')
    except ex.OptNotFound:
        pass

    try:
        kwargs['zone'] = svc.conf_get_string_scope(s, 'zone')
    except ex.OptNotFound:
        pass

    try:
        kwargs['container_rid'] = svc.conf_get_string_scope(s, 'container_rid')
    except ex.OptNotFound:
        pass

    if rtype == "docker":
        try:
            kwargs['network'] = svc.conf_get_string_scope(s, 'network')
        except ex.OptNotFound:
            pass
        try:
            kwargs['del_net_route'] = svc.conf_get_boolean_scope(s, 'del_net_route')
        except ex.OptNotFound:
            pass

    if rtype == "crossbow":
        if 'zone' in kwargs:
            svc.log.error("'zone' and 'type=crossbow' are incompatible in section %s"%s)
            return
        ip = __import__('resIpCrossbow')
    elif 'zone' in kwargs:
        ip = __import__('resIpZone')
    elif rtype == "docker" or "container_rid" in kwargs:
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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_md(svc, s):
    kwargs = {}

    try:
        kwargs['uuid'] = svc.conf_get_string_scope(s, 'uuid')
    except ex.OptNotFound:
        svc.log.error("uuid must be set in section %s"%s)
        return

    try:
        kwargs['shared'] = svc.conf_get_string_scope(s, 'shared')
    except ex.OptNotFound:
        if len(svc.nodes|svc.drpnodes) < 2:
            kwargs['shared'] = False
            svc.log.debug("md %s shared param defaults to %s due to single node configuration"%(s, kwargs['shared']))
        else:
            l = [ p for p in svc.config.options(s) if "@" in p ]
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_drbd(svc, s):
    """Parse the configuration file and add a drbd object for each [drbd#n]
    section. Drbd objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['res'] = svc.conf_get_string(s, 'res')
    except ex.OptNotFound:
        svc.log.error("res must be set in section %s"%s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_vdisk(svc, s):
    kwargs = {}
    devpath = {}

    for attr, val in conf.items(s):
        if 'path@' in attr:
            devpath[attr.replace('path@','')] = val

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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_stonith(svc, s):
    if rcEnv.nodename in svc.drpnodes:
        # no stonith on DRP nodes
        return

    kwargs = {}

    try:
        _type = svc.conf_get_string(s, 'type')
        if len(_type) > 1:
            _type = _type[0].upper()+_type[1:].lower()
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%s)
        return

    if _type in ('Ilo'):
        try:
            kwargs['name'] = svc.conf_get_string_scope(s, 'name')
        except ex.OptNotFound:
            pass
        try:
            kwargs['name'] = svc.conf_get_string_scope(s, 'target')
        except ex.OptNotFound:
            pass

        if 'name' not in kwargs:
            svc.log.error("target must be set in section %s"%s)
            return
    elif _type in ('Callout'):
        try:
            kwargs['cmd'] = svc.conf_get_string_scope(s, 'cmd')
        except ex.OptNotFound:
            pass

        if 'cmd' not in kwargs:
            svc.log.error("cmd must be set in section %s"%s)
            return

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_loop(svc, s):
    """Parse the configuration file and add a loop object for each [loop#n]
    section. Loop objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['loopFile'] = svc.conf_get_string_scope(s, 'file')
    except ex.OptNotFound:
        svc.log.error("file must be set in section %s"%s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r


def add_disk_disk(svc, s):
    kwargs = {}
    try:
        kwargs['disk_id'] = svc.conf_get_string_scope(s, 'disk_id')
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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_disk_gce(svc, s):
    kwargs = {}
    try:
        kwargs['names'] = svc.conf_get_string_scope(s, 'names').split()
    except ex.OptNotFound:
        svc.log.error("names must be set in section %s"%s)
        return

    try:
        kwargs['gce_zone'] = svc.conf_get_string_scope(s, 'gce_zone')
    except ex.OptNotFound:
        svc.log.error("gce_zone must be set in section %s"%s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_disk_amazon(svc, s):
    kwargs = {}
    try:
        kwargs['volumes'] = svc.conf_get_string_scope(s, 'volumes').split()
    except ex.OptNotFound:
        svc.log.error("volumes must be set in section %s"%s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_rados(svc, s):
    kwargs = {}
    try:
        kwargs['images'] = svc.conf_get_string_scope(s, 'images').split()
    except ex.OptNotFound:
        pass
    try:
        kwargs['keyring'] = svc.conf_get_string_scope(s, 'keyring')
    except ex.OptNotFound:
        pass
    try:
        kwargs['client_id'] = svc.conf_get_string_scope(s, 'client_id')
    except ex.OptNotFound:
        pass
    try:
        lock_shared_tag = svc.conf_get_string_scope(s, 'lock_shared_tag')
    except ex.OptNotFound:
        lock_shared_tag = None
    try:
        lock = svc.conf_get_string_scope(s, 'lock')
    except ex.OptNotFound:
        lock = None

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
    add_triggers_and_requires(svc, r, s)
    svc += r

    if not lock:
        return

    # rados locking resource
    kwargs["rid"] = kwargs["rid"]+"lock"
    kwargs["lock"] = lock
    kwargs["lock_shared_tag"] = lock_shared_tag
    r = m.DiskLock(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r


def add_raw(svc, s):
    kwargs = {}
    disk_type = "Raw"+rcEnv.sysname
    try:
        zone = svc.conf_get_string_scope(s, 'zone')
    except:
        zone = None
    try:
        kwargs['user'] = svc.conf_get_string_scope(s, 'user')
    except ex.OptNotFound:
        pass
    try:
        kwargs['group'] = svc.conf_get_string_scope(s, 'group')
    except ex.OptNotFound:
        pass
    try:
        kwargs['perm'] = svc.conf_get_string_scope(s, 'perm')
    except ex.OptNotFound:
        pass
    try:
        kwargs['create_char_devices'] = svc.conf_get_boolean_scope(s, 'create_char_devices')
    except ex.OptNotFound:
        pass
    try:
        devs = svc.conf_get_string_scope(s, 'devs')
        if zone is not None:
            devs = devs.replace(":", ":<%s>" % zone)
        kwargs['devs'] = set(devs.split())
    except ex.OptNotFound:
        svc.log.error("devs must be set in section %s"%s)
        return

    # backward compat : the dummy keyword is deprecated in favor of
    # the standard "noaction" tag.
    try:
        dummy = svc.conf_get_boolean_scope(s, 'dummy')
    except ex.OptNotFound:
        dummy = False

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
    if dummy:
        r.tags.add("noaction")
    if zone is not None:
        r.tags.add('zone')
        r.tags.add(zone)
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_gandi(svc, s):
    disk_type = "Gandi"
    kwargs = {}
    try:
        kwargs['cloud_id'] = svc.conf_get_string_scope(s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return
    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return
    try:
        kwargs['node'] = svc.conf_get_string_scope(s, 'node')
    except ex.OptNotFound:
        pass
    try:
        kwargs['user'] = svc.conf_get_string_scope(s, 'user')
    except ex.OptNotFound:
        pass
    try:
        kwargs['group'] = svc.conf_get_string_scope(s, 'user')
    except ex.OptNotFound:
        pass
    try:
        kwargs['perm'] = svc.conf_get_string_scope(s, 'perm')
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

    try:
        m = __import__('resDisk'+disk_type)
    except ImportError:
        svc.log.error("disk type %s is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_disk_compat(svc, s):
    try:
        disk_type = svc.conf_get_string_scope(s, 'type')
    except ex.OptNotFound:
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
        add_rados(svc, s)
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
    try:
        # deprecated keyword 'vgname'
        kwargs['name'] = svc.conf_get_string_scope(s, 'vgname')
    except ex.OptNotFound:
        pass
    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        if "name" not in kwargs:
            svc.log.error("name must be set in section %s"%s)
            return
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_vg(svc, s):
    try:
        add_disk_compat(svc, s)
        return
    except ex.OptNotFound:
        pass

    disk_type = rcEnv.sysname
    kwargs = {}
    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'vgname')
    except ex.OptNotFound:
        pass
    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        if "name" not in kwargs:
            svc.log.error("name must be set in section %s"%s)
            return
    try:
        kwargs['dsf'] = svc.conf_get_boolean_scope(s, 'dsf')
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

    try:
        m = __import__('resDiskVg'+disk_type)
    except ImportError:
        svc.log.error("disk type %s is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_sync(svc, s):
    try:
        rtype = svc.conf_get_string_scope(s, 'type')
    except ex.OptNotFound:
        rtype = "rsync"
    globals()["add_sync_"+rtype](svc, s)

def add_container(svc, s):
    rtype = svc.conf_get_string_scope(s, 'type')
    globals()["add_container_"+rtype](svc, s)

def add_disk(svc, s):
    """Parse the configuration file and add a disk object for each [disk#n]
    section. Disk objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        disk_type = svc.conf_get_string_scope(s, 'type')
    except ex.OptNotFound:
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
        add_rados(svc, s)
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

    try:
        kwargs['container_id'] = svc.conf_get_string_scope(s, 'container_id')
    except ex.OptNotFound:
        svc.log.error("container_id must be set in section %s"%s)
        return

    if not conf.has_section(kwargs['container_id']):
        svc.log.error("%s.container_id points to an invalid section"%kwargs['container_id'])
        return

    try:
        container_type = svc.conf_get_string_scope(kwargs['container_id'], 'type')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_zpool(svc, s):
    """Parse the configuration file and add a zpool object for each disk.zpool
    section. Pools objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'poolname')
    except ex.OptNotFound:
        pass

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        pass

    if "name" not in kwargs:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        zone = svc.conf_get_string_scope(s, 'zone')
    except ex.OptNotFound:
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

    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_share(svc, s):
    try:
        _type = svc.conf_get_string_scope(s, 'type')
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%s)
        return

    fname = 'add_share_'+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s))
    globals()[fname](svc, s)

def add_share_nfs(svc, s):
    kwargs = {}

    try:
        kwargs['path'] = svc.conf_get_string_scope(s, 'path')
    except ex.OptNotFound:
        svc.log.error("path must be set in section %s"%s)
        return

    try:
        kwargs['opts'] = svc.conf_get_string_scope(s, 'opts')
    except ex.OptNotFound:
        svc.log.error("opts must be set in section %s"%s)
        return

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

    add_triggers_and_requires(svc, r, s)
    svc += r

def add_fs_directory(svc, s):
    kwargs = {}

    try:
        kwargs['path'] = svc.conf_get_string_scope(s, 'path')
    except ex.OptNotFound:
        svc.log.error("path must be set in section %s"%s)
        return

    try:
        kwargs['user'] = svc.conf_get_string_scope(s, 'user')
    except ex.OptNotFound:
        pass

    try:
        kwargs['group'] = svc.conf_get_string_scope(s, 'group')
    except ex.OptNotFound:
        pass

    try:
        kwargs['perm'] = svc.conf_get_string_scope(s, 'perm')
    except ex.OptNotFound:
        pass

    try:
        zone = svc.conf_get_string_scope(s, 'zone')
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

    add_triggers_and_requires(svc, r, s)
    svc += r

def add_fs(svc, s):
    """Parse the configuration file and add a fs object for each [fs#n]
    section. Fs objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['fs_type'] = svc.conf_get_string_scope(s, 'type')
    except ex.OptNotFound:
        kwargs['fs_type'] = ""

    if kwargs['fs_type'] == "directory":
        add_fs_directory(svc, s)
        return

    try:
        kwargs['device'] = svc.conf_get_string_scope(s, 'dev')
    except ex.OptNotFound:
        svc.log.error("dev must be set in section %s"%s)
        return

    try:
        kwargs['mount_point'] = svc.conf_get_string_scope(s, 'mnt')
    except ex.OptNotFound:
        svc.log.error("mnt must be set in section %s"%s)
        return

    if kwargs['mount_point'][-1] != "/" and kwargs['mount_point'][-1] == '/':
        """ Remove trailing / to not risk losing rsync src trailing /
            upon snap mountpoint substitution.
        """
        kwargs['mount_point'] = kwargs['mount_point'][0:-1]

    try:
        kwargs['mount_options'] = svc.conf_get_string_scope(s, 'mnt_opt')
    except ex.OptNotFound:
        kwargs['mount_options'] = ""

    try:
        kwargs['snap_size'] = svc.conf_get_int_scope(s, 'snap_size')
    except ex.OptNotFound:
        pass

    try:
        zone = svc.conf_get_string_scope(s, 'zone')
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

    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_esx(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_hpvm(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_ldom(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_vbox(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_xen(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_zone(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['delete_on_stop'] = svc.conf_get_boolean_scope(s, 'delete_on_stop')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)



def add_container_vcloud(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cloud_id'] = svc.conf_get_string_scope(s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return

    try:
        kwargs['vapp'] = svc.conf_get_string_scope(s, 'vapp')
    except ex.OptNotFound:
        svc.log.error("vapp must be set in section %s"%s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_amazon(svc, s):
    kwargs = {}

    # mandatory keywords
    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['cloud_id'] = svc.conf_get_string_scope(s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return

    # optional keywords
    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
        pass

    # provisioning keywords
    try:
        kwargs['image_id'] = svc.conf_get_string_scope(s, 'image_id')
    except ex.OptNotFound:
        pass

    try:
        kwargs['size'] = svc.conf_get_string_scope(s, 'size')
    except ex.OptNotFound:
        pass

    try:
        kwargs['key_name'] = svc.conf_get_string_scope(s, 'key_name')
    except ex.OptNotFound:
        pass

    try:
        kwargs['subnet'] = svc.conf_get_string_scope(s, 'subnet')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_openstack(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cloud_id'] = svc.conf_get_string_scope(s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return

    try:
        kwargs['size'] = svc.conf_get_string_scope(s, 'size')
    except ex.OptNotFound:
        svc.log.error("size must be set in section %s"%s)
        return

    try:
        kwargs['key_name'] = svc.conf_get_string_scope(s, 'key_name')
    except ex.OptNotFound:
        svc.log.error("key_name must be set in section %s"%s)
        return

    try:
        kwargs['shared_ip_group'] = svc.conf_get_string_scope(s, 'shared_ip_group')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_vz(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_kvm(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_srp(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_lxc(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cf'] = svc.conf_get_string_scope(s, 'cf')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_docker(svc, s):
    kwargs = {}

    try:
        kwargs['run_image'] = svc.conf_get_string_scope(s, 'run_image')
    except ex.OptNotFound:
        svc.log.error("'run_image' parameter is mandatory in section %s"%s)
        return

    try:
        kwargs['run_command'] = svc.conf_get_string_scope(s, 'run_command')
    except ex.OptNotFound:
        pass

    try:
        kwargs['run_args'] = svc.conf_get_string_scope(s, 'run_args')
    except ex.OptNotFound:
        pass

    try:
        kwargs['docker_service'] = svc.conf_get_boolean_scope(s, 'docker_service')
    except ex.OptNotFound:
        pass

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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_ovm(svc, s):
    kwargs = {}

    try:
        kwargs['uuid'] = svc.conf_get_string_scope(s, 'uuid')
    except ex.OptNotFound:
        svc.log.error("uuid must be set in section %s"%s)
        return

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['guestos'] = svc.conf_get_string_scope(s, 'guestos')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_container_jail(svc, s):
    kwargs = {}

    try:
        kwargs['jailroot'] = svc.conf_get_string_scope(s, 'jailroot')
    except ex.OptNotFound:
        svc.log.error("jailroot must be set in section %s"%s)
        return

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['ips'] = svc.conf_get_string_scope(s, 'ips').split()
    except ex.OptNotFound:
        pass

    try:
        kwargs['ip6s'] = svc.conf_get_string_scope(s, 'ip6s').split()
    except ex.OptNotFound:
        pass

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
    add_triggers_and_requires(svc, r, s)
    svc += r
    add_scsireserv(svc, r, s)

def add_mandatory_syncs(svc):
    """Mandatory files to sync:
    1/ to all nodes: service definition
    2/ to drpnodes: system files to replace on the drpnode in case of startdrp
    """

    """1
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
        targethash = {'nodes': svc.nodes, 'drpnodes': svc.drpnodes}
        kwargs['rid'] = "sync#i0"
        kwargs['src'] = src
        kwargs['dst'] = dst
        kwargs['options'] = ['-R']+exclude
        if svc.config.has_option(kwargs['rid'], 'options'):
            kwargs['options'] += cmdline2list(svc.config.get(kwargs['rid'], 'options'))
        kwargs['target'] = targethash
        kwargs['internal'] = True
        kwargs['disabled'] = get_disabled(svc, kwargs['rid'])
        kwargs['optional'] = get_optional(svc, kwargs['rid'])
        kwargs.update(get_sync_args(svc, kwargs['rid']))
        r = resSyncRsync.Rsync(**kwargs)
        svc += r

    """2
    """
    if len(svc.drpnodes) == 0:
        return

    targethash = {'drpnodes': svc.drpnodes}
    """ Reparent all PRD backed-up file in drp_path/node on the drpnode
    """
    dst = os.path.join(rcEnv.paths.drp_path, rcEnv.nodename)
    i = 0
    for src, exclude in rcEnv.drp_sync_files:
        """'-R' triggers rsync relative mode
        """
        kwargs = {}
        src = [ s for s in src if os.path.exists(s) ]
        if len(src) == 0:
            continue
        i += 1
        kwargs['rid'] = "sync#i"+str(i)
        kwargs['src'] = src
        kwargs['dst'] = dst
        kwargs['options'] = ['-R']+exclude
        if svc.config.has_option(kwargs['rid'], 'options'):
            kwargs['options'] += cmdline2list(svc.config.get(kwargs['rid'], 'options'))
        kwargs['target'] = targethash
        kwargs['internal'] = True
        kwargs['disabled'] = get_disabled(svc, kwargs['rid'])
        kwargs['optional'] = get_optional(svc, kwargs['rid'])
        kwargs.update(get_sync_args(svc, kwargs['rid']))
        r = resSyncRsync.Rsync(**kwargs)
        svc += r

def add_sync_docker(svc, s):
    kwargs = {}

    try:
        kwargs['target'] = svc.conf_get_string_scope(s, 'target').split(' ')
    except ex.OptNotFound:
        return

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

    try:
        kwargs['src'] = svc.conf_get_string_scope(s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    try:
        kwargs['dst'] = svc.conf_get_string_scope(s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['target'] = svc.conf_get_string_scope(s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

    try:
        kwargs['recursive'] = svc.conf_get_boolean(s, 'recursive')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    btrfs = __import__('resSyncBtrfs')
    r = btrfs.SyncBtrfs(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_zfs(svc, s):
    kwargs = {}

    try:
        kwargs['src'] = svc.conf_get_string_scope(s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    try:
        kwargs['dst'] = svc.conf_get_string_scope(s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['target'] = svc.conf_get_string_scope(s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

    try:
        kwargs['recursive'] = svc.conf_get_boolean(s, 'recursive')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    zfs = __import__('resSyncZfs')
    r = zfs.SyncZfs(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_dds(svc, s):
    kwargs = {}

    try:
        kwargs['src'] = svc.conf_get_string_scope(s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    dsts = {}
    for node in svc.nodes | svc.drpnodes:
        dst = svc.conf_get_string_scope(s, 'dst', impersonate=node)
        dsts[node] = dst

    if len(dsts) == 0:
        for node in svc.nodes | svc.drpnodes:
            dsts[node] = kwargs['src']

    kwargs['dsts'] = dsts

    try:
        kwargs['target'] = svc.conf_get_string_scope(s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

    try:
        kwargs['sender'] = svc.conf_get_string(s, 'sender')
    except ex.OptNotFound:
        pass

    try:
        kwargs['snap_size'] = svc.conf_get_int_scope(s, 'snap_size')
    except ex.OptNotFound:
        pass

    try:
        kwargs['delta_store'] = svc.conf_get_string_scope(s, 'delta_store')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    dds = __import__('resSyncDds')
    r = dds.syncDds(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_dcsckpt(svc, s):
    kwargs = {}

    try:
        kwargs['dcs'] = set(svc.conf_get_string_scope(s, 'dcs').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'dcs' set" % s)
        return

    try:
        kwargs['manager'] = set(svc.conf_get_string_scope(s, 'manager').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'manager' set" % s)
        return

    import json
    pairs = []
    if 'pairs' in conf.options(s):
        try:
            pairs = json.loads(conf.get(s, 'pairs'))
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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_dcssnap(svc, s):
    kwargs = {}

    try:
        kwargs['dcs'] = set(svc.conf_get_string(s, 'dcs').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'dcs' set" % s)
        return

    try:
        kwargs['manager'] = set(svc.conf_get_string(s, 'manager').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'manager' set" % s)
        return

    try:
        kwargs['snapname'] = set(svc.conf_get_string(s, 'snapname').split())
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_s3(svc, s):
    kwargs = {}

    try:
        kwargs['full_schedule'] = svc.conf_get_string_scope(s, 'full_schedule')
    except ex.OptNotFound:
        pass

    try:
        kwargs['options'] = svc.conf_get_string_scope(s, 'options').split()
    except ex.OptNotFound:
        pass

    try:
        kwargs['snar'] = svc.conf_get_string_scope(s, 'snar')
    except ex.OptNotFound:
        pass

    try:
        kwargs['bucket'] = svc.conf_get_string_scope(s, 'bucket')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have bucket set" % s)
        return

    try:
        kwargs['src'] = svc.conf_get_string_scope(s, 'src').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__('resSyncS3')
    r = sc.syncS3(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_zfssnap(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keep'] = svc.conf_get_int_scope(s, 'keep')
    except ex.OptNotFound:
        pass

    try:
        kwargs['recursive'] = svc.conf_get_boolean_scope(s, 'recursive')
    except ex.OptNotFound:
        pass

    try:
        kwargs['dataset'] = svc.conf_get_string_scope(s, 'dataset').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dataset set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__('resSyncZfsSnap')
    r = sc.syncZfsSnap(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_btrfssnap(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string_scope(s, 'name')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keep'] = svc.conf_get_int_scope(s, 'keep')
    except ex.OptNotFound:
        pass

    try:
        kwargs['subvol'] = svc.conf_get_string_scope(s, 'subvol').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have subvol set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))
    sc = __import__('resSyncBtrfsSnap')
    r = sc.syncBtrfsSnap(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_necismsnap(svc, s):
    kwargs = {}

    try:
        kwargs['array'] = svc.conf_get_string_scope(s, 'array')
    except ex.OptNotFound:
        pass

    try:
        kwargs['devs'] = svc.conf_get_string_scope(s, 'devs')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_evasnap(svc, s):
    kwargs = {}

    try:
        kwargs['eva_name'] = svc.conf_get_string(s, 'eva_name')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have eva_name set" % s)
        return

    try:
        kwargs['snap_name'] = svc.conf_get_string(s, 'snap_name')
    except ex.OptNotFound:
        kwargs['snap_name'] = svc.svcname

    import json
    pairs = []
    if 'pairs' in conf.options(s):
        pairs = json.loads(conf.get(s, 'pairs'))
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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_hp3parsnap(svc, s):
    kwargs = {}

    try:
        kwargs['array'] = svc.conf_get_string_scope(s, 'array')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have array set" % s)
        return

    try:
        vv_names = svc.conf_get_string_scope(s, 'vv_names').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have vv_names set" % s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_hp3par(svc, s):
    kwargs = {}

    try:
        kwargs['mode'] = svc.conf_get_string_scope(s, 'mode')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have mode set" % s)
        return

    try:
        kwargs['array'] = svc.conf_get_string_scope(s, 'array')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have array set" % s)
        return

    rcg_names = {}
    for node in svc.nodes | svc.drpnodes:
        array = svc.conf_get_string_scope(s, 'array', impersonate=node)
        rcg = svc.conf_get_string_scope(s, 'rcg', impersonate=node)
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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_symsrdfs(svc, s):
    kwargs = {}

    try:
        kwargs['symdg'] = svc.conf_get_string(s, 'symdg')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have symdg set" % s)
        return

    try:
        kwargs['rdfg'] = svc.conf_get_int(s, 'rdfg')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have rdfg number set" % s)
        return

    try:
        kwargs['symid'] = svc.conf_get_string_scope(s, 'symid')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have symid" % s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r


def add_sync_radosclone(svc, s):
    kwargs = {}

    try:
        kwargs['client_id'] = svc.conf_get_string_scope(s, 'client_id')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keyring'] = svc.conf_get_string_scope(s, 'keyring')
    except ex.OptNotFound:
        pass

    try:
        kwargs['pairs'] = svc.conf_get_string_scope(s, 'pairs').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have pairs set" % s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_radossnap(svc, s):
    kwargs = {}

    try:
        kwargs['client_id'] = svc.conf_get_string_scope(s, 'client_id')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keyring'] = svc.conf_get_string_scope(s, 'keyring')
    except ex.OptNotFound:
        pass

    try:
        kwargs['images'] = svc.conf_get_string_scope(s, 'images').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have images set" % s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_symsnap(svc, s):
    _add_sync_symclone(svc, s, "sync.symsnap")

def add_sync_symclone(svc, s):
    _add_sync_symclone(svc, s, "sync.symclone")

def _add_sync_symclone(svc, s, t):
    kwargs = {}
    kwargs['type'] = t
    try:
        kwargs['pairs'] = svc.conf_get_string(s, 'pairs').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have pairs set" % s)
        return

    try:
        kwargs['symid'] = svc.conf_get_string_scope(s, 'symid')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have sid set" % s)
        return

    try:
        kwargs['recreate_timeout'] = svc.conf_get_int(s, 'recreate_timeout')
    except ex.OptNotFound:
        pass

    try:
        kwargs['consistent'] = svc.conf_get_boolean(s, 'consistent')
    except ex.OptNotFound:
        pass

    try:
        kwargs['precopy'] = svc.conf_get_boolean(s, 'precopy')
    except ex.OptNotFound:
        pass

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_ibmdssnap(svc, s):
    kwargs = {}

    try:
        kwargs['pairs'] = svc.conf_get_string(s, 'pairs').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have pairs set" % s)
        return

    try:
        kwargs['array'] = svc.conf_get_string(s, 'array')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have array set" % s)
        return

    try:
        kwargs['bgcopy'] = svc.conf_get_boolean(s, 'bgcopy')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have bgcopy set" % s)
        return

    try:
        kwargs['recording'] = svc.conf_get_boolean(s, 'recording')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have recording set" % s)
        return

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_nexenta(svc, s):
    kwargs = {}

    try:
        kwargs['name'] = svc.conf_get_string(s, 'name')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'name' set" % s)
        return

    try:
        kwargs['path'] = svc.conf_get_string_scope(s, 'path')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have path set" % s)
        return

    try:
        kwargs['reversible'] = svc.conf_get_boolean_scope(s, "reversible")
    except:
        pass

    filers = {}
    if 'filer' in conf.options(s):
        for n in svc.nodes | svc.drpnodes:
            filers[n] = conf.get(s, 'filer')
    if 'filer@nodes' in conf.options(s):
        for n in svc.nodes:
            filers[n] = conf.get(s, 'filer@nodes')
    if 'filer@drpnodes' in conf.options(s):
        for n in svc.nodes:
            filers[n] = conf.get(s, 'filer@drpnodes')
    for o in conf.options(s):
        if 'filer@' not in o:
            continue
        (filer, node) = o.split('@')
        if node in ('nodes', 'drpnodes'):
            continue
        filers[node] = conf.get(s, o)
    if rcEnv.nodename not in filers:
        svc.log.error("config file section %s must have filer@%s set" %(s, rcEnv.nodename))

    kwargs['filers'] = filers
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    import resSyncNexenta
    r = resSyncNexenta.syncNexenta(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_netapp(svc, s):
    kwargs = {}

    try:
        kwargs['path'] = svc.conf_get_string_scope(s, 'path')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have path set" % s)
        return

    try:
        kwargs['user'] = svc.conf_get_string_scope(s, 'user')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have user set" % s)
        return

    filers = {}
    if 'filer' in conf.options(s):
        for n in svc.nodes | svc.drpnodes:
            filers[n] = conf.get(s, 'filer')
    if 'filer@nodes' in conf.options(s):
        for n in svc.nodes:
            filers[n] = conf.get(s, 'filer@nodes')
    if 'filer@drpnodes' in conf.options(s):
        for n in svc.nodes:
            filers[n] = conf.get(s, 'filer@drpnodes')
    for o in conf.options(s):
        if 'filer@' not in o:
            continue
        (filer, node) = o.split('@')
        if node in ('nodes', 'drpnodes'):
            continue
        filers[node] = conf.get(s, o)
    if rcEnv.nodename not in filers:
        svc.log.error("config file section %s must have filer@%s set" %(s, rcEnv.nodename))

    kwargs['filers'] = filers
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    import resSyncNetapp
    r = resSyncNetapp.syncNetapp(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_sync_rsync(svc, s):
    if s.startswith("sync#i"):
        # internal syncs have their own dedicated add function
        return

    options = []
    kwargs = {}
    kwargs['src'] = []
    try:
        _s = svc.conf_get_string_scope(s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    for src in _s.split():
        kwargs['src'] += glob.glob(src)

    try:
        kwargs['dst'] = svc.conf_get_string_scope(s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['dstfs'] = svc.conf_get_string_scope(s, 'dstfs')
    except ex.OptNotFound:
        pass

    try:
        _s = svc.conf_get_string_scope(s, 'options')
        options += _s.split()
    except ex.OptNotFound:
        pass

    try:
        # for backward compat (use options keyword now)
        _s = svc.conf_get_string_scope(s, 'exclude')
        options += _s.split()
    except ex.OptNotFound:
        pass

    kwargs['options'] = options

    try:
        kwargs['snap'] = svc.conf_get_boolean_scope(s, 'snap')
    except ex.OptNotFound:
        pass

    try:
        _s = svc.conf_get_string_scope(s, 'target')
        target = _s.split()
    except ex.OptNotFound:
        target = []

    try:
        kwargs['bwlimit'] = svc.conf_get_int_scope(s, 'bwlimit')
    except ex.OptNotFound:
        pass

    targethash = {}
    if 'nodes' in target: targethash['nodes'] = svc.nodes
    if 'drpnodes' in target: targethash['drpnodes'] = svc.drpnodes
    kwargs['target'] = targethash
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(svc, s)
    kwargs['tags'] = get_tags(svc, s)
    kwargs['disabled'] = get_disabled(svc, s)
    kwargs['optional'] = get_optional(svc, s)
    kwargs.update(get_sync_args(svc, s))

    r = resSyncRsync.Rsync(**kwargs)
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_task(svc, s):
    kwargs = {}

    try:
        kwargs['command'] = svc.conf_get_string_scope(s, 'command')
    except ex.OptNotFound:
        svc.log.error("'command' is not defined in config file section %s"%s)
        return

    try:
        kwargs['on_error'] = svc.conf_get_string_scope(s, 'on_error')
    except ex.OptNotFound:
        pass

    try:
        kwargs['user'] = svc.conf_get_string_scope(s, 'user')
    except ex.OptNotFound:
        pass

    try:
        kwargs['confirmation'] = svc.conf_get_boolean_scope(s, 'confirmation')
    except ex.OptNotFound:
        pass

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
    add_triggers_and_requires(svc, r, s)
    svc += r

def add_app(svc, s):
    resApp = ximport('resApp')
    kwargs = {}

    try:
        kwargs['script'] = svc.conf_get_string_scope(s, 'script')
    except ex.OptNotFound:
        svc.log.error("'script' is not defined in config file section %s"%s)
        return

    try:
        kwargs['start'] = svc.conf_get_int_scope(s, 'start')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'start'))
        return

    try:
        kwargs['stop'] = svc.conf_get_int_scope(s, 'stop')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'stop'))
        return

    try:
        kwargs['check'] = svc.conf_get_int_scope(s, 'check')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'check'))
        return

    try:
        kwargs['info'] = svc.conf_get_int_scope(s, 'info')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'info'))
        return

    try:
        kwargs['timeout'] = svc.conf_get_int_scope(s, 'timeout')
    except ex.OptNotFound:
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
    add_triggers_and_requires(svc, r, s)
    svc += r


def setup_logging(svcnames):
    """Setup logging to stream + logfile, and logfile rotation
    class Logger instance name: 'log'
    """
    global log
    max_svcname_len = 0

    # compute max svcname length to align logging stream output
    for svcname in svcnames:
        n = len(svcname)
        if n > max_svcname_len:
            max_svcname_len = n

    rcLogger.max_svcname_len = max_svcname_len
    log = rcLogger.initLogger('init')

def build(name, minimal=False, svcconf=None):
    """build(name) is in charge of Svc creation
    it return None if service Name is not managed by local node
    else it return new Svc instance
    """
    #
    # node discovery is hidden in a separate module to
    # keep it separate from the framework stuff
    #
    discover_node()
    svc = Svc(svcname=name)

    try:
        encapnodes = [n.lower() for n in svc.conf_get_string_scope('DEFAULT', "encapnodes").split() if n != ""]
    except ex.OptNotFound:
        encapnodes = []
    svc.encapnodes = set(encapnodes)

    try:
        nodes = [n.lower() for n in svc.conf_get_string_scope('DEFAULT', "nodes").split() if n != ""]
    except ex.OptNotFound:
        nodes = [rcEnv.nodename]
    svc.ordered_nodes = nodes
    svc.nodes = set(nodes)

    try:
        drpnodes = [n.lower() for n in svc.conf_get_string_scope('DEFAULT', "drpnodes").split() if n != ""]
    except ex.OptNotFound:
        drpnodes = []

    try:
        drpnode = svc.conf_get_string_scope('DEFAULT', "drpnode").lower()
        if drpnode not in drpnodes and drpnode != "":
            drpnodes.append(drpnode)
    except ex.OptNotFound:
        drpnode = ''
    svc.ordered_drpnodes = drpnodes
    svc.drpnodes = set(drpnodes)

    try:
        flex_primary = svc.conf_get_string_scope('DEFAULT', "flex_primary").lower()
    except ex.OptNotFound:
        if len(nodes) > 0:
            flex_primary = nodes[0]
        else:
            flex_primary = ''
    svc.flex_primary = flex_primary

    try:
        drp_flex_primary = svc.conf_get_string_scope('DEFAULT', "drp_flex_primary").lower()
    except ex.OptNotFound:
        if len(drpnodes) > 0:
            drp_flex_primary = drpnodes[0]
        else:
            drp_flex_primary = ''
    svc.drp_flex_primary = drp_flex_primary


    #
    # Store and validate the service type
    #
    if svc.conf_has_option_scoped("DEFAULT", "env"):
        svc.svc_env = svc.conf_get_string_scope('DEFAULT', "env")
    elif svc.conf_has_option_scoped("DEFAULT", "service_type"):
        svc.svc_env = svc.conf_get_string_scope('DEFAULT', "service_type")

    try:
        svc.lock_timeout = svc.conf_get_int_scope('DEFAULT', 'lock_timeout')
    except ex.OptNotFound:
        pass

    if svc.config.has_option('DEFAULT', 'disable'):
        svc.disabled = svc.config.getboolean("DEFAULT", "disable")
    else:
        pass

    try:
        svc.presnap_trigger = svc.conf_get_string_scope('DEFAULT', 'presnap_trigger').split()
    except ex.OptNotFound:
        pass

    try:
        svc.postsnap_trigger = svc.conf_get_string_scope('DEFAULT', 'postsnap_trigger').split()
    except ex.OptNotFound:
        pass

    try:
        svc.disable_rollback = not svc.conf_get_boolean_scope('DEFAULT', "rollback")
    except ex.OptNotFound:
        pass

    if rcEnv.nodename in svc.encapnodes:
        svc.encap = True
    else:
        svc.encap = False

    #
    # amazon options
    #
    try:
        svc.aws = svc.conf_get_string_scope("DEFAULT", 'aws')
    except ex.OptNotFound:
        pass

    try:
        svc.aws_profile = svc.conf_get_string_scope("DEFAULT", 'aws_profile')
    except ex.OptNotFound:
        pass

    #
    # process group options
    #
    try:
        svc.create_pg = svc.conf_get_boolean_scope("DEFAULT", 'create_pg')
    except ex.OptNotFound:
        pass

    try:
        anti_affinity = svc.conf_get_string_scope('DEFAULT', 'anti_affinity')
        svc.anti_affinity = set(svc.conf_get_string_scope('DEFAULT', 'anti_affinity').split())
    except ex.OptNotFound:
        pass

    try:
        svc.clustertype = svc.conf_get_string_scope('DEFAULT', 'cluster_type')
    except ex.OptNotFound:
        pass

    if 'flex' in svc.clustertype:
        svc.ha = True

    try:
        svc.flex_min_nodes = svc.conf_get_int_scope('DEFAULT', 'flex_min_nodes')
    except ex.OptNotFound:
        svc.flex_min_nodes = 1
    if svc.flex_min_nodes < 0:
        svc.flex_min_nodes = 0
    nb_nodes = len(svc.nodes|svc.drpnodes)
    if svc.flex_min_nodes > nb_nodes:
        svc.flex_min_nodes = nb_nodes

    try:
        svc.flex_max_nodes = svc.conf_get_int_scope('DEFAULT', 'flex_max_nodes')
    except ex.OptNotFound:
        svc.flex_max_nodes = nb_nodes
    if svc.flex_max_nodes < nb_nodes:
        svc.flex_max_nodes = nb_nodes
    if svc.flex_max_nodes < svc.flex_min_nodes:
        svc.flex_max_nodes = svc.flex_min_nodes

    try:
        svc.flex_cpu_low_threshold = svc.conf_get_int_scope('DEFAULT', 'flex_cpu_low_threshold')
    except ex.OptNotFound:
        svc.flex_cpu_low_threshold = 10
    if svc.flex_cpu_low_threshold < 0:
        raise ex.excInitError("invalid flex_cpu_low_threshold '%d' (<0)."%svc.flex_cpu_low_threshold)
    if svc.flex_cpu_low_threshold > 100:
        raise ex.excInitError("invalid flex_cpu_low_threshold '%d' (>100)."%svc.flex_cpu_low_threshold)

    try:
        svc.flex_cpu_high_threshold = svc.conf_get_int_scope('DEFAULT', 'flex_cpu_high_threshold')
    except ex.OptNotFound:
        svc.flex_cpu_high_threshold = 90
    if svc.flex_cpu_high_threshold < 0:
        raise ex.excInitError("invalid flex_cpu_high_threshold '%d' (<0)."%svc.flex_cpu_high_threshold)
    if svc.flex_cpu_high_threshold > 100:
        raise ex.excInitError("invalid flex_cpu_high_threshold '%d' (>100)."%svc.flex_cpu_high_threshold)

    try:
        svc.show_disabled = svc.conf_get_boolean_scope('DEFAULT', 'show_disabled')
    except ex.OptNotFound:
        svc.show_disabled = True

    """ prune service whose service type does not match host mode
    """
    if svc.svc_env != 'PRD' and rcEnv.node_env == 'PRD':
        raise ex.excInitError('not allowed to run on this node (svc env=%s node env=%s)' % (svc.svc_env, rcEnv.node_env))

    try:
        svc.drp_type = svc.conf_get_string_scope('DEFAULT', 'drp_type')
    except ex.OptNotFound:
        pass

    try:
        svc.comment = svc.conf_get_string_scope('DEFAULT', 'comment')
    except ex.OptNotFound:
        pass

    try:
        svc.monitor_action = svc.conf_get_string_scope('DEFAULT', "monitor_action")
    except ex.OptNotFound:
        pass

    try:
        svc.pre_monitor_action = svc.conf_get_string_scope('DEFAULT', "pre_monitor_action")
    except ex.OptNotFound:
        pass

    try:
        svc.app = svc.conf_get_string_scope('DEFAULT', "app")
    except ex.OptNotFound:
        pass

    try:
        svc.drnoaction = svc.conf_get_boolean_scope('DEFAULT', "drnoaction")
    except ex.OptNotFound:
        pass

    try:
        svc.bwlimit = svc.conf_get_int_scope('DEFAULT', "bwlimit")
    except ex.OptNotFound:
        svc.bwlimit = None

    try:
        svc.clustername = svc.conf_get_string_scope('DEFAULT', "cluster")
    except ex.OptNotFound:
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
    s = list(map(lambda x: os.path.basename(x)[:-5], s))

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

    if type(svcnames) == str:
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
        except (ex.excError, ex.excInitError) as e:
            errors.append("%s: %s" % (name, str(e)))
            svclog = rcLogger.initLogger(name, handlers=["file", "syslog"])
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
    return [ s for n, s in sorted(services.items()) ], errors

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
            del(d['rid'])
            if section in sections:
                sections[section].update(d)
            else:
                sections[section] = d
        elif 'rtype' in d and d["rtype"] == "env":
            del(d["rtype"])
            if "env" in sections:
                sections["env"].update(d)
            else:
                sections["env"] = d
        elif 'rtype' in d and d["rtype"] != "DEFAULT":
            if 'rid' in d:
               del(d['rid'])
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
                del(d["rtype"])
            defaults.update(d)

    from svcDict import KeyDict, MissKeyNoDefault, KeyInvalidValue
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
        p = os.readlink(src)
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

