#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import os
import sys
import ConfigParser
import logging
import re
import socket

from rcGlobalEnv import *
from rcNode import discover_node
from rcUtilities import *
import rcLogger
import resSyncRsync
import rcExceptions as ex

check_privs()

#
# file tree abstraction
#
rcEnv.pathsvc = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
rcEnv.pathbin = os.path.join(rcEnv.pathsvc, 'bin')
rcEnv.pathetc = os.path.join(rcEnv.pathsvc, 'etc')
rcEnv.pathlib = os.path.join(rcEnv.pathsvc, 'lib')
rcEnv.pathlog = os.path.join(rcEnv.pathsvc, 'log')
rcEnv.pathtmp = os.path.join(rcEnv.pathsvc, 'tmp')
rcEnv.pathvar = os.path.join(rcEnv.pathsvc, 'var')
rcEnv.pathlock = os.path.join(rcEnv.pathvar, 'lock')
rcEnv.sysname, rcEnv.nodename, x, x, rcEnv.machine = os.uname()
rcEnv.nodename = socket.gethostname()

os.environ['LANG'] = 'C'
os.environ['PATH'] += ':/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'

def conf_get(svc, conf, s, o, t, scope=False):
    if t == 'string':
        f = conf.get
    elif t == 'boolean':
        f = conf.getboolean
    elif t == 'integer':
        f = conf.getint
    else:
        raise Exception()

    if not scope:
        if conf.has_option(s, o):
            return f(s, o)
        else:
            raise ex.OptNotFound

    if conf.has_option(s, o+"@"+rcEnv.nodename):
        return f(s, o+"@"+rcEnv.nodename)
    elif conf.has_option(s, o+"@nodes") and \
         rcEnv.nodename in svc.nodes:
        return f(s, o+"@nodes")
    elif conf.has_option(s, o+"@drpnodes") and \
         rcEnv.nodename in svc.drpnodes:
        return f(s, o+"@drpnodes")
    elif conf.has_option(s, o):
        return f(s, o)
    else:
        raise ex.OptNotFound

def conf_get_string(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'string', scope=False)

def conf_get_string_scope(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'string', scope=True)

def conf_get_boolean(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'boolean', scope=False)

def conf_get_boolean_scope(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'boolean', scope=True)

def conf_get_int(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'integer', scope=False)

def conf_get_int_scope(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'integer', scope=True)

def svcmode_mod_name(svcmode=''):
    """Returns (moduleName, serviceClassName) implementing the class for
    a given service mode. For example:
    lxc    => ('svcLxc', 'SvcLxc')
    zone   => ('svcZone', 'SvcZone')
    hosted => ('svcHosted', 'SvcHosted')
    """
    if svcmode == 'lxc':
        return ('svcLxc', 'SvcLxc')
    elif svcmode == 'vz':
        return ('svcVz', 'SvcVz')
    elif svcmode == 'zone':
        return ('svcZone', 'SvcZone')
    elif svcmode == 'jail':
        return ('svcJail', 'SvcJail')
    elif svcmode == 'hosted':
        return ('svcHosted', 'SvcHosted')
    elif svcmode == 'hpvm':
        return ('svcHpVm', 'SvcHpVm')
    elif svcmode == 'ldom':
        return ('svcLdom', 'SvcLdom')
    elif svcmode == 'kvm':
        return ('svcKvm', 'SvcKvm')
    elif svcmode == 'esx':
        return ('svcEsx', 'SvcEsx')
    elif svcmode == 'xen':
        return ('svcXen', 'SvcXen')
    elif svcmode == 'ovm':
        return ('svcOvm', 'SvcOvm')
    elif svcmode == 'vbox':
        return ('svcVbox', 'SvcVbox')
    raise

def get_tags(conf, section):
    if conf.has_option(section, 'tags'):
        return set(conf.get(section, "tags").split())
    return set([])

def get_optional(conf, section, svc):
    if not conf.has_section(section):
        if conf.has_option('DEFAULT', 'optional'):
            return conf.getboolean("DEFAULT", "optional")
        else:
            return False
    if conf.has_option(section, 'optional'):
        return conf.getboolean(section, "optional")
    nodes = set([])
    if conf.has_option(section, 'optional_on'):
        l = conf.get(section, "optional_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i])
    if rcEnv.nodename in nodes:
        return True
    return False

def get_monitor(conf, section, svc):
    if not conf.has_section(section):
        if conf.has_option('DEFAULT', 'monitor'):
            return conf.getboolean("DEFAULT", "monitor")
        else:
            return False
    if conf.has_option(section, 'monitor'):
        return conf.getboolean(section, "monitor")
    nodes = set([])
    if conf.has_option(section, 'monitor_on'):
        l = conf.get(section, "monitor_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i])
    if rcEnv.nodename in nodes:
        return True
    return False

def get_disabled(conf, section, svc):
    if not conf.has_section(section):
        if conf.has_option('DEFAULT', 'disable'):
            return conf.getboolean("DEFAULT", "disable")
        else:
            return False
    if conf.has_option(section, 'disable'):
        return conf.getboolean(section, "disable")
    nodes = set([])
    if conf.has_option(section, 'disable_on'):
        l = conf.get(section, "disable_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i])
    if rcEnv.nodename in nodes:
        return True
    return False

def need_scsireserv(svc, conf, section):
    """scsireserv = true can be set globally or in a specific
    resource section
    """
    r = False
    try:
        r = conf_get_boolean_scope(svc, conf, section, 'scsireserv')
    except ex.OptNotFound:
        defaults = conf.defaults()
        if 'scsireserv' in defaults:
            r = bool(defaults['scsireserv'])
    return r

def add_scsireserv(svc, resource, conf, section):
    if not need_scsireserv(svc, conf, section):
        return
    try:
        sr = __import__('resScsiReserv'+rcEnv.sysname)
    except ImportError:
        sr = __import__('resScsiReserv')

    kwargs = {}
    kwargs['rid'] = resource.rid
    kwargs['tags'] = resource.tags
    kwargs['disks'] = resource.disklist()
    kwargs['disabled'] = resource.is_disabled()
    kwargs['optional'] = resource.is_optional()

    r = sr.ScsiReserv(**kwargs)
    svc += r

def add_triggers(resource, conf, section):
    if conf.has_option(section, 'pre_stop'):
        resource.pre_stop = conf.get(section, 'pre_stop').split()
    if conf.has_option(section, 'pre_start'):
        resource.pre_start = conf.get(section, 'pre_start').split()
    if conf.has_option(section, 'pre_syncnodes'):
        resource.pre_syncnodes = conf.get(section, 'pre_syncnodes').split()
    if conf.has_option(section, 'pre_syncdrp'):
        resource.pre_syncdrp = conf.get(section, 'pre_syncdrp').split()
    if conf.has_option(section, 'post_stop'):
        resource.post_stop = conf.get(section, 'post_stop').split()
    if conf.has_option(section, 'post_start'):
        resource.post_start = conf.get(section, 'post_start').split()
    if conf.has_option(section, 'post_syncnodes'):
        resource.post_syncnodes = conf.get(section, 'post_syncnodes').split()
    if conf.has_option(section, 'post_syncdrp'):
        resource.post_syncdrp = conf.get(section, 'post_syncdrp').split()

def always_on_nodes_set(svc, conf, section):
    try:
        always_on_opt = conf.get(section, "always_on").split()
    except:
        always_on_opt = []
    always_on = set([])
    if 'nodes' in always_on_opt:
        always_on |= svc.nodes
    if 'drpnodes' in always_on_opt:
        always_on |= svc.drpnodes
    always_on |= set(always_on_opt) - set(['nodes', 'drpnodes'])
    return always_on

def get_sync_args(conf, s, svc):
    kwargs = {}
    defaults = conf.defaults()
    if conf.has_option(s, 'sync_max_delay'):
        kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
    elif 'sync_max_delay' in defaults:
        kwargs['sync_max_delay'] = int(defaults['sync_max_delay'])

    if conf.has_option(s, 'sync_min_delay'):
        kwargs['sync_interval'] = conf.getint(s, 'sync_min_delay')
        svc.log.warn("sync_min_delay is deprecated. replace with sync_interval.")
    elif 'sync_min_delay' in defaults:
        kwargs['sync_interval'] = int(defaults['sync_min_delay'])
        svc.log.warn("sync_min_delay is deprecated. replace with sync_interval.")

    if conf.has_option(s, 'sync_interval'):
        kwargs['sync_interval'] = conf.getint(s, 'sync_interval')
    elif 'sync_interval' in defaults:
        kwargs['sync_interval'] = int(defaults['sync_interval'])
    elif 'sync_max_delay' in kwargs:
        kwargs['sync_interval'] = kwargs['sync_max_delay']

    import json

    try:
        if conf.has_option(s, 'sync_period'):
            kwargs['sync_period'] = json.loads(conf.get(s, 'sync_period'))
        elif conf.has_option('DEFAULT', 'sync_period'):
            kwargs['sync_period'] = json.loads(conf.get('DEFAULT', 'sync_period'))
    except ValueError:
        svc.log.error("malformed parameter value: %s.%s"%(s, 'sync_period'))

    try:
        if conf.has_option(s, 'sync_days'):
            kwargs['sync_days'] = json.loads(conf.get(s, 'sync_days'))
        elif conf.has_option('DEFAULT', 'sync_days'):
            kwargs['sync_days'] = json.loads(conf.get('DEFAULT', 'sync_days'))
    except ValueError:
        svc.log.error("malformed parameter value: %s.%s"%(s, 'sync_days'))

    return kwargs

def add_ips(svc, conf):
    """Parse the configuration file and add an ip object for each [ip#n]
    section. Ip objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('ip#[0-9]', s, re.I) is None:
            continue

        kwargs = {}

        try:
            kwargs['ipName'] = conf_get_string_scope(svc, conf, s, 'ipname')
        except ex.OptNotFound:
            svc.log.error("nor ipname and ipname@%s defined in config file section %s"%(rcEnv.nodename, s))
            continue

        try:
            kwargs['ipDev'] = conf_get_string_scope(svc, conf, s, 'ipdev')
        except ex.OptNotFound:
            svc.log.debug('add_ips ipdev not found in ip section %s'%s)
            continue

        if hasattr(svc, "vmname"):
            vmname = svc.vmname
        else:
            vmname = svc.svcname
        try:
            kwargs['mask'] = conf_get_string_scope(svc, conf, s, 'netmask')
        except ex.OptNotFound:
            pass

        if svc.svcmode == 'lxc':
            ip = __import__('resIp'+rcEnv.sysname+'Lxc')
        elif svc.svcmode == 'vz':
            ip = __import__('resIp'+rcEnv.sysname+'Lxc')
        elif svc.svcmode  == 'kvm':
            ip = __import__('resIp'+'Kvm')
        elif svc.svcmode  == 'hpvm':
            ip = __import__('resIp'+'HpVm')
        elif svc.svcmode  == 'ldom':
            ip = __import__('resIp'+'Ldom')
        elif svc.svcmode  == 'zone':
            ip = __import__('resIp'+'Zone')
        elif svc.svcmode  == 'xen' or svc.svcmode  == 'ovm':
            ip = __import__('resIp'+'Xen')
        elif svc.svcmode  == 'esx':
            ip = __import__('resIp'+'Esx')
        elif svc.svcmode  == 'vbox':
            ip = __import__('resIp'+'Vbox')
        else:
            ip = __import__('resIp'+rcEnv.sysname)
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs['monitor'] = get_monitor(conf, s, svc)
        r = ip.Ip(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_drbds(svc, conf):
    """Parse the configuration file and add a drbd object for each [drbd#n]
    section. Drbd objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('drbd#[0-9]', s, re.I) is None:
            continue

        kwargs = {}

        try:
            kwargs['res'] = conf_get_string(svc, conf, s, 'res')
        except ex.OptNotFound:
            svc.log.error("res must be set in section %s"%s)
            return

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs['monitor'] = get_monitor(conf, s, svc)
        drbd = __import__('resDrbd')
        r = drbd.Drbd(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_vdisks(svc, conf):
    for s in conf.sections():
        if re.match('vdisk#[0-9]', s, re.I) is None:
            continue

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
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs['monitor'] = get_monitor(conf, s, svc)
        vdisk = __import__('resVdisk')
        r = vdisk.Vdisk(**kwargs)
        add_triggers(r, conf, s)
        svc += r
        add_scsireserv(svc, r, conf, s)

def add_stoniths(svc, conf):
    for s in conf.sections():
        if re.match('stonith#[0-9]', s, re.I) is None:
            continue

        if rcEnv.nodename in svc.drpnodes:
            # no stonith on DRP nodes
            return

        kwargs = {}

        try:
            _type = conf_get_string(svc, conf, s, 'type')
            if len(_type) > 1:
                _type = _type[0].upper()+_type[1:].lower()
        except ex.OptNotFound:
            svc.log.error("type must be set in section %s"%s)
            return

        if _type in ('Ilo'):
            try:
                kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
            except ex.OptNotFound:
                pass
            try:
                kwargs['name'] = conf_get_string_scope(svc, conf, s, 'target')
            except ex.OptNotFound:
                pass
    
            if 'name' not in kwargs:
                svc.log.error("target must be set in section %s"%s)
                return

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)

        st = __import__('resStonith'+_type)
        try:
            st = __import__('resStonith'+_type)
        except ImportError:
            svc.log.error("resStonith%s is not implemented"%_type)
            continue

        r = st.Stonith(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_hbs(svc, conf):
    for s in conf.sections():
        if re.match('hb#[0-9]', s, re.I) is None:
            continue

        if rcEnv.nodename in svc.drpnodes:
            # no heartbeat on DRP nodes
            return

        kwargs = {}

        try:
            hbtype = conf_get_string(svc, conf, s, 'type')
        except ex.OptNotFound:
            svc.log.error("type must be set in section %s"%s)
            return

        try:
            kwargs['name'] = conf_get_string(svc, conf, s, 'name')
        except ex.OptNotFound:
            if hbtype == 'openha':
                svc.log.error("name must be set in section %s"%s)
                return

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)

        try:
            hb = __import__('resHb'+hbtype)
        except ImportError:
            svc.log.error("resHb%s is not implemented"%hbtype)
            continue

        r = hb.Hb(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_loops(svc, conf):
    """Parse the configuration file and add a loop object for each [loop#n]
    section. Loop objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('loop#[0-9]', s, re.I) is None:
            continue

        kwargs = {}

        try:
            kwargs['loopFile'] = conf_get_string_scope(svc, conf, s, 'file')
        except ex.OptNotFound:
            svc.log.error("file must be set in section %s"%s)
            return

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs['monitor'] = get_monitor(conf, s, svc)

        try:
            loop = __import__('resLoop'+rcEnv.sysname)
        except ImportError:
            svc.log.error("resLoop%s is not implemented"%rcEnv.sysname)
            continue

        r = loop.Loop(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_vgs(svc, conf):
    """Parse the configuration file and add a vg object for each [vg#n]
    section. Vg objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('vg#[0-9]', s, re.I) is None:
            continue

        kwargs = {}

        try:
            vgtype = conf_get_string_scope(svc, conf, s, 'vgtype')
            if len(vgtype) > 2:
                vgtype = vgtype[0].upper() + vgtype[1:].lower()
        except ex.OptNotFound:
            vgtype = rcEnv.sysname

        try:
            vgtype = conf_get_string_scope(svc, conf, s, 'type')
            if len(vgtype) > 2:
                vgtype = vgtype[0].upper() + vgtype[1:].lower()
        except ex.OptNotFound:
            vgtype = rcEnv.sysname

        if vgtype == 'Raw':
            vgtype += rcEnv.sysname

        try:
            kwargs['name'] = conf_get_string_scope(svc, conf, s, 'vgname')
        except ex.OptNotFound:
            if not vgtype.startswith("Raw"):
                svc.log.error("vgname must be set in section %s"%s)
                return

        try:
            kwargs['dsf'] = conf_get_boolean_scope(svc, conf, s, 'dsf')
        except ex.OptNotFound:
            pass

        try:
            kwargs['devs'] = set(conf_get_string_scope(svc, conf, s, 'devs').split())
        except ex.OptNotFound:
            if vgtype == "Raw":
                svc.log.error("devs must be set in section %s"%s)
                return

        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs['monitor'] = get_monitor(conf, s, svc)

        try:
            vg = __import__('resVg'+vgtype)
        except ImportError:
            svc.log.error("vg type %s is not implemented"%vgtype)
            continue

        r = vg.Vg(**kwargs)
        add_triggers(r, conf, s)
        svc += r
        add_scsireserv(svc, r, conf, s)

def add_vmdg(svc, conf):
    if not conf.has_section('vmdg'):
        return
    if svc.svcmode == 'hpvm':
        vg = __import__('resVgHpVm')
    elif svc.svcmode == 'ldom':
        vg = __import__('resVgLdom')
    elif svc.svcmode in rcEnv.vt_libvirt:
        vg = __import__('resVgLibvirtVm')
    else:
        return

    kwargs = {}
    kwargs['rid'] = 'vmdg'
    kwargs['tags'] = get_tags(conf, 'vmdg')
    kwargs['name'] = 'vmdg'
    kwargs['disabled'] = get_disabled(conf, 'vmdg', svc)
    kwargs['optional'] = get_optional(conf, 'vmdg', svc)
    kwargs['monitor'] = get_monitor(conf, 'vmdg', svc)

    r = vg.Vg(**kwargs)
    add_triggers(r, conf, 'vmdg')
    svc += r
    add_scsireserv(svc, r, conf, 'vmdg')

def add_pools(svc, conf):
    """Parse the configuration file and add a pool object for each [pool#n]
    section. Pools objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('pool#[0-9]', s, re.I) is None:
            continue

        kwargs = {}

        try:
            kwargs['name'] = conf_get_string_scope(svc, conf, s, 'poolname')
        except ex.OptNotFound:
            svc.log.error("poolname must be set in section %s"%s)
            continue

        pool = __import__('resVgZfs')

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs['monitor'] = get_monitor(conf, s, svc)

        r = pool.Pool(**kwargs)
        add_triggers(r, conf, s)
        svc += r
        add_scsireserv(svc, r, conf, s)

def add_filesystems(svc, conf):
    """Parse the configuration file and add a fs object for each [fs#n]
    section. Fs objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('fs#[0-9]', s, re.I) is None:
            continue

        kwargs = {}

        try:
            kwargs['device'] = conf_get_string_scope(svc, conf, s, 'dev')
        except ex.OptNotFound:
            svc.log.error("dev must be set in section %s"%s)
            continue

        try:
            kwargs['mountPoint'] = conf_get_string_scope(svc, conf, s, 'mnt')
        except ex.OptNotFound:
            svc.log.error("mnt must be set in section %s"%s)
            continue

        if kwargs['mountPoint'][-1] == '/':
            """ Remove trailing / to not risk losing rsync src trailing /
                upon snap mountpoint substitution.
            """
            kwargs['mountPoint'] = kwargs['mountPoint'][0:-1]

        try:
            kwargs['fsType'] = conf_get_string_scope(svc, conf, s, 'type')
        except ex.OptNotFound:
            kwargs['fsType'] = ""

        try:
            kwargs['mntOpt'] = conf_get_string_scope(svc, conf, s, 'mnt_opt')
        except ex.OptNotFound:
            kwargs['mntOpt'] = ""

        try:
            kwargs['snap_size'] = conf_get_int_scope(svc, conf, s, 'snap_size')
        except ex.OptNotFound:
            pass

        if svc.svcmode == 'zone':
            try:
                globalfs = conf.getboolean(s, "globalfs")
            except:
                globalfs = False

            if globalfs is False:
                kwargs['mountPoint'] = os.path.realpath(svc.zone.zonepath+'/root/'+kwargs['mountPoint'])

        try:
            mount = __import__('resMount'+rcEnv.sysname)
        except ImportError:
            svc.log.error("resMount%s is not implemented"%rcEnv.sysname)
            continue

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs['monitor'] = get_monitor(conf, s, svc)

        r = mount.Mount(**kwargs)
        add_triggers(r, conf, s)
        svc += r
        add_scsireserv(svc, r, conf, s)

def add_mandatory_syncs(svc, conf):
    """Mandatory files to sync:
    1/ to all nodes: service definition
    2/ to drpnodes: system files to replace on the drpnode in case of startdrp
    """

    """1
    """
    kwargs = {}
    src = []
    src.append(os.path.join(rcEnv.pathetc, svc.svcname))
    src.append(os.path.join(rcEnv.pathetc, svc.svcname+'.env'))
    src.append(os.path.join(rcEnv.pathetc, svc.svcname+'.d'))
    localrc = os.path.join(rcEnv.pathetc, svc.svcname+'.dir')
    cluster = os.path.join(rcEnv.pathetc, svc.svcname+'.cluster')
    if os.path.exists(cluster):
        src.append(cluster)
    if os.path.exists(localrc):
        src.append(localrc)
    for rs in svc.resSets:
        for r in rs.resources:
            src += r.files_to_sync()
    dst = os.path.join("/")
    exclude = ['--exclude=*.core']
    targethash = {'nodes': svc.nodes, 'drpnodes': svc.drpnodes}
    kwargs['rid'] = "sync#i0"
    kwargs['src'] = src
    kwargs['dst'] = dst
    kwargs['options'] = ['-R']+exclude
    kwargs['target'] = targethash
    kwargs['internal'] = True
    kwargs['disabled'] = get_disabled(conf, kwargs['rid'], svc)
    kwargs['optional'] = get_optional(conf, kwargs['rid'], svc)
    kwargs.update(get_sync_args(conf, 'sync', svc))
    r = resSyncRsync.Rsync(**kwargs)
    svc += r

    """2
    """
    targethash = {'drpnodes': svc.drpnodes}
    """ Reparent all PRD backed-up file in drp_path/node on the drpnode
    """
    dst = os.path.join(rcEnv.drp_path, rcEnv.nodename)
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
        kwargs['target'] = targethash
        kwargs['internal'] = True
        kwargs['disabled'] = get_disabled(conf, kwargs['rid'], svc)
        kwargs['optional'] = get_optional(conf, kwargs['rid'], svc)
        kwargs.update(get_sync_args(conf, 'sync', svc))
        r = resSyncRsync.Rsync(**kwargs)
        svc += r

def add_syncs(svc, conf):
    add_syncs_rsync(svc, conf)
    add_syncs_netapp(svc, conf)
    add_syncs_nexenta(svc, conf)
    add_syncs_symclone(svc, conf)
    add_syncs_evasnap(svc, conf)
    add_syncs_dds(svc, conf)
    add_syncs_zfs(svc, conf)

def add_syncs_zfs(svc, conf):
    zfs = __import__('resSyncZfs')
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if not conf.has_option(s, 'type'):
            continue
        elif conf.get(s, 'type') != 'zfs':
            continue

        kwargs = {}

        try:
            kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have src set" % s)
            continue

        try:
            kwargs['dst'] = conf_get_string_scope(svc, conf, s, 'dst')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have dst set" % s)
            continue

        try:
            kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split()
        except ex.OptNotFound:
            svc.log.error("config file section %s must have target set" % s)
            continue

        try:
            kwargs['recursive'] = conf_get_boolean(svc, conf, s, 'recursive')
        except ex.OptNotFound:
            pass

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs.update(get_sync_args(conf, s, svc))
        r = zfs.SyncZfs(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_dds(svc, conf):
    dds = __import__('resSyncDds')
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if not conf.has_option(s, 'type'):
            continue
        elif conf.get(s, 'type') != 'dds':
            continue

        kwargs = {}

        try:
            kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have src set" % s)
            continue

        try:
            kwargs['dst'] = conf_get_string_scope(svc, conf, s, 'dst')
        except ex.OptNotFound:
            kwargs['dst'] = conf.get(s, 'src')

        try:
            kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split()
        except ex.OptNotFound:
            svc.log.error("config file section %s must have target set" % s)
            continue

        try:
            kwargs['sender'] = conf_get_string(svc, conf, s, 'sender')
        except ex.OptNotFound:
            pass

        try:
            kwargs['snap_size'] = conf_get_int_scope(svc, conf, s, 'snap_size')
        except ex.OptNotFound:
            pass

        try:
            kwargs['delta_store'] = conf_get_string_scope(svc, conf, s, 'delta_store')
        except ex.OptNotFound:
            pass

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs.update(get_sync_args(conf, s, svc))
        r = dds.syncDds(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_evasnap(svc, conf):
    try:
        sc = __import__('resSyncEvasnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncEvasnap')
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if not conf.has_option(s, 'type'):
            continue
        elif conf.get(s, 'type') != 'evasnap':
            continue

        kwargs = {}

        try:
            kwargs['eva_name'] = conf_get_string(svc, conf, s, 'eva_name')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have eva_name set" % s)
            continue

        import json
        pairs = []
        if 'pairs' in conf.options(s):
            pairs = json.loads(conf.get(s, 'pairs'))
        if len(pairs) == 0:
            svc.log.error("config file section %s must have pairs set" % s)
            continue
        else:
            kwargs['pairs'] = pairs

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs.update(get_sync_args(conf, s, svc))
        r = sc.syncEvasnap(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_symclone(svc, conf):
    try:
        sc = __import__('resSyncSymclone'+rcEnv.sysname)
    except:
        sc = __import__('resSyncSymclone')
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if not conf.has_option(s, 'type'):
            continue
        elif conf.get(s, 'type') != 'symclone':
            continue

        kwargs = {}

        try:
            kwargs['symdg'] = conf_get_string(svc, conf, s, 'symdg')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have symdg set" % s)
            continue

        try:
            kwargs['symdevs'] = conf_get_string_scope(svc, conf, s, 'symdevs').split()
        except ex.OptNotFound:
            svc.log.error("config file section %s must have symdevs set" % s)
            continue

        try:
            kwargs['precopy_timeout'] = conf_get_int(svc, conf, s, 'precopy_timeout')
        except ex.OptNotFound:
            pass

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs.update(get_sync_args(conf, s, svc))
        r = sc.syncSymclone(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_nexenta(svc, conf):
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        kwargs = {}

        if not conf.has_option(s, 'type'):
            continue
        elif conf.get(s, 'type') != 'nexenta':
            continue

        try:
            kwargs['name'] = conf_get_string(svc, conf, s, 'name')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have 'name' set" % s)
            continue

        try:
            kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have path set" % s)
            continue

        try:
            kwargs['reversible'] = conf.getboolean(s, "reversible")
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
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs.update(get_sync_args(conf, s, svc))

        import resSyncNexenta
        r = resSyncNexenta.syncNexenta(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_netapp(svc, conf):
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        kwargs = {}

        if not conf.has_option(s, 'type'):
            continue
        elif conf.get(s, 'type') != 'netapp':
            continue

        try:
            kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have path set" % s)
            continue

        try:
            kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
        except ex.OptNotFound:
            svc.log.error("config file section %s must have user set" % s)
            continue

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
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs.update(get_sync_args(conf, s, svc))

        import resSyncNetapp
        r = resSyncNetapp.syncNetapp(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_rsync(svc, conf):
    """Add mandatory node-to-nodes and node-to-drpnode synchronizations, plus
    the those described in the config file.
    """
    import glob
    add_mandatory_syncs(svc, conf)

    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if conf.has_option(s, 'type') and \
           conf.get(s, 'type') != 'rsync':
            continue

        if not conf.has_option(s, 'src') or \
           not conf.has_option(s, 'dst'):
            svc.log.error("config file section %s must have src and dst set" % s)
            return

        options = []
        kwargs = {}
        kwargs['src'] = []
        for src in conf.get(s, "src").split():
            kwargs['src'] += glob.glob(src)
        kwargs['dst'] = conf.get(s, "dst")

        if conf.has_option(s, 'dstfs'):
            kwargs['dstfs'] = conf.get(s, 'dstfs')

        if conf.has_option(s, 'options'):
            options += conf.get(s, 'options').split()
        if conf.has_option(s, 'exclude'):
            # for backward compat (use options keyword now)
            options += conf.get(s, 'exclude').split()
        kwargs['options'] = options

        if conf.has_option(s, 'snap'):
            kwargs['snap'] = conf.getboolean(s, 'snap')

        if conf.has_option(s, 'target'):
            target = conf.get(s, 'target').split()
        else:
            target = []

        if conf.has_option(s, 'bwlimit'):
            kwargs['bwlimit'] = conf.get(s, 'bwlimit')

        targethash = {}
        if 'nodes' in target: targethash['nodes'] = svc.nodes
        if 'drpnodes' in target: targethash['drpnodes'] = svc.drpnodes
        kwargs['target'] = targethash
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s, svc)
        kwargs.update(get_sync_args(conf, s, svc))

        r = resSyncRsync.Rsync(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_apps(svc, conf):
    if svc.svcmode in rcEnv.vt_supported:
        resApp = __import__('resAppVm')
    else:
        resApp = __import__('resApp')

    kwargs = {}
    kwargs['runmethod'] = svc.runmethod

    s = 'app'
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
       
    r = resApp.Apps(**kwargs)
    svc += r

def setup_logging():
    """Setup logging to stream + logfile, and logfile rotation
    class Logger instance name: 'log'
    """
    global log
    log = rcLogger.initLogger('INIT')

def build(name):
    """build(name) is in charge of Svc creation
    it return None if service Name is not managed by local node
    else it return new Svc instance
    """
    svcconf = os.path.join(rcEnv.pathetc, name) + '.env'
    svcinitd = os.path.join(rcEnv.pathetc, name) + '.d'
    logfile = os.path.join(rcEnv.pathlog, name) + '.log'
    rcEnv.logfile = logfile

    setup_logging()

    #
    # node discovery is hidden in a separate module to
    # keep it separate from the framework stuff
    #
    discover_node()

    #
    # parse service configuration file
    # class RawConfigParser instance name: 'conf'
    #
    svcmode = "hosted"
    conf = None
    kwargs = {'svcname': name}
    if os.path.isfile(svcconf):
        conf = ConfigParser.RawConfigParser()
        conf.read(svcconf)
        defaults = conf.defaults()
        if "mode" in defaults:
            svcmode = defaults["mode"]
        if "vm_name" in defaults:
            if svcmode not in rcEnv.vt_supported:
                log.error("can not set 'vm_name' with '%s' mode in %s env"%(svcmode, name))
                return None
            vmname = defaults["vm_name"]
            kwargs['vmname'] = vmname
        if "vm_uuid" in defaults:
            if svcmode != "ovm":
                log.error("can not set 'vm_uuid' with '%s' mode in %s env"%(svcmode, name))
                return None
            kwargs['vmuuid'] = defaults["vm_uuid"]
        if "guest_os" in defaults and \
           len(defaults["guest_os"]) > 0:
            if svcmode not in rcEnv.vt_supported:
                log.error("can not set 'guest_os' with '%s' mode in %s env"%(svcmode, name))
                return None
            guestos = defaults["guest_os"]
            kwargs['guestos'] = guestos
        elif svcmode in rcEnv.vt_supported:
            guestos = rcEnv.sysname
            kwargs['guestos'] = guestos
        if svcmode == 'jail':
            if not "jailroot" in defaults:
                log.error("jailroot parameter is mandatory for jail mode")
                return None
            jailroot = defaults["jailroot"]
            if not os.path.exists(jailroot):
                log.error("jailroot %s does not exist"%jailroot)
                return None
            kwargs['jailroot'] = jailroot
        kwargs['disabled'] = get_disabled(conf, "", "")

    #
    # dynamically import the module matching the service mode
    # and instanciate a service
    #
    log.debug('service mode = ' + svcmode)
    mod , svc_class_name = svcmode_mod_name(svcmode)
    svcMod = __import__(mod)
    svc = getattr(svcMod, svc_class_name)(**kwargs)
    svc.svcmode = svcmode
    if "presnap_trigger" in defaults:
        svc.presnap_trigger = defaults["presnap_trigger"].split()
    if "postsnap_trigger" in defaults:
        svc.postsnap_trigger = defaults["postsnap_trigger"].split()

    #
    # containerization options
    #
    if "containerize" in defaults:
        svc.containerize = bool(defaults["containerize"])
    if "container_cpus" in defaults:
        svc.container_cpus = defaults["container_cpus"]
    if "container_cpu_shares" in defaults:
        svc.container_cpu_shares = defaults["container_cpu_shares"]
    if "container_mems" in defaults:
        svc.container_mems = defaults["container_mems"]
    if "container_mem_limit" in defaults:
        svc.container_mem_limit = defaults["container_mem_limit"]
    if "container_vmem_limit" in defaults:
        svc.container_vmem_limit = defaults["container_vmem_limit"]

    #
    # Store useful properties
    #
    svc.logfile = logfile
    svc.conf = svcconf
    svc.initd = svcinitd
    svc.config = conf

    #
    # Setup service properties from config file content
    #

    if "nodes" in defaults:
        svc.nodes = set(defaults["nodes"].split())
        svc.nodes -= set([''])
    else:
        svc.nodes = set([])

    if "drpnodes" in defaults:
        svc.drpnodes = set(defaults["drpnodes"].split())
        svc.drpnodes -= set([''])
    else:
        svc.drpnodes = set([])

    if "drpnode" in defaults:
        svc.drpnode = defaults["drpnode"]
        svc.drpnodes |= set([svc.drpnode])
        svc.drpnodes -= set([''])
    else:
        svc.drpnode = ''

    """ prune not managed service
    """
    if rcEnv.nodename not in svc.nodes | svc.drpnodes:
        svc.log.error('service %s not managed here' % name)
        del(svc)
        return None

    if "cluster_type" in defaults:
        svc.clustertype = defaults["cluster_type"]
    else:
        svc.clustertype = 'failover'
    if 'flex' in svc.clustertype:
        svc.ha = True
    allowed_clustertype = ['failover', 'allactive', 'flex', 'autoflex']
    if svc.clustertype not in allowed_clustertype:
        svc.log.error("invalid cluster type '%s'. allowed: %s"%(svc.svcname, svc.clustertype, ', '.join(allowed_clustertype)))
        del(svc)
        return None

    if "flex_primary" in defaults:
        svc.flex_primary = defaults["flex_primary"]
    else:
        svc.flex_primary = ''

    if "flex_min_nodes" in defaults:
        svc.flex_min_nodes = int(defaults["flex_min_nodes"])
    else:
        svc.flex_min_nodes = 1
    if svc.flex_min_nodes < 1:
        svc.log.error("invalid flex_min_nodes '%d' (<1)."%svc.flex_min_nodes)
        del(svc)
        return None
    nb_nodes = len(svc.nodes)
    if nb_nodes > 0 and svc.flex_min_nodes > nb_nodes:
        svc.log.error("invalid flex_min_nodes '%d' (>%d nb of nodes)."%(svc.flex_min_nodes, nb_nodes))
        del(svc)
        return None

    if "flex_max_nodes" in defaults:
        svc.flex_max_nodes = int(defaults["flex_max_nodes"])
    else:
        svc.flex_max_nodes = 0
    if svc.flex_max_nodes < 0:
        svc.log.error("invalid flex_max_nodes '%d' (<0)."%svc.flex_max_nodes)
        del(svc)
        return None

    if "flex_cpu_low_threshold" in defaults:
        svc.flex_cpu_low_threshold = int(defaults["flex_cpu_low_threshold"])
    else:
        svc.flex_cpu_low_threshold = 10
    if svc.flex_cpu_low_threshold < 0:
        svc.log.error("invalid flex_cpu_low_threshold '%d' (<0)."%svc.flex_cpu_low_threshold)
        del(svc)
        return None
    if svc.flex_cpu_low_threshold > 100:
        svc.log.error("invalid flex_cpu_low_threshold '%d' (>100)."%svc.flex_cpu_low_threshold)
        del(svc)
        return None

    if "flex_cpu_high_threshold" in defaults:
        svc.flex_cpu_high_threshold = int(defaults["flex_cpu_high_threshold"])
    else:
        svc.flex_cpu_high_threshold = 90
    if svc.flex_cpu_high_threshold < 0:
        svc.log.error("invalid flex_cpu_high_threshold '%d' (<0)."%svc.flex_cpu_high_threshold)
        del(svc)
        return None
    if svc.flex_cpu_high_threshold > 100:
        svc.log.error("invalid flex_cpu_high_threshold '%d' (>100)."%svc.flex_cpu_high_threshold)
        del(svc)
        return None

    if "service_type" in defaults:
        svc.svctype = defaults["service_type"]
    else:
        svc.svctype = ''

    if svc.svctype not in rcEnv.allowed_svctype:
        svc.log.error('service %s type %s is not a known service type (%s)'%(svc.svcname, svc.svctype, ', '.join(allowed_svctype)))
        del(svc)
        return None

    """ prune service whose service type does not match host mode
    """
    if svc.svctype != 'PRD' and rcEnv.host_mode == 'PRD':
        svc.log.error('service %s type %s is not allowed to run on this node (host mode %s)' % (svc.svcname, svc.svctype, rcEnv.host_mode))
        del(svc)
        return None

    if "autostart_node" in defaults:
        svc.autostart_node = defaults["autostart_node"].split()
    else:
        svc.autostart_node = []

    if "drp_type" in defaults:
        svc.drp_type = defaults["drp_type"]
    else:
        svc.drp_type = ''

    if "comment" in defaults:
        svc.comment = defaults["comment"]
    else:
        svc.comment = ''

    if "monitor_action" in defaults:
        svc.monitor_action = defaults["monitor_action"]

    if "app" in defaults:
        svc.app = defaults["app"]
    else:
        svc.app = ''

    if "drnoaction" in defaults:
        svc.drnoaction = defaults["drnoaction"]
    else:
        svc.drnoaction = False

    if "bwlimit" in defaults:
        svc.bwlimit = defaults["bwlimit"]
    else:
        svc.bwlimit = None

    #
    # instanciate resources
    #
    try:
        add_hbs(svc, conf)
        add_stoniths(svc, conf)
        add_ips(svc, conf)
        add_drbds(svc, conf)
        add_loops(svc, conf)
        add_vdisks(svc, conf)
        add_vgs(svc, conf)
        add_vmdg(svc, conf)
        add_pools(svc, conf)
        add_filesystems(svc, conf)
        add_apps(svc, conf)
        add_syncs(svc, conf)
    except (ex.excInitError, ex.excError), e:
        log.error(str(e))
        return None

    return svc

def is_service(f):
    svcmgr = os.path.join(rcEnv.pathsvc, 'bin', 'svcmgr')
    if os.path.realpath(f) != os.path.realpath(svcmgr):
        return False
    if not os.path.exists(f + '.env'):
        return False
    return True

def list_services():
    if not os.path.exists(rcEnv.pathetc):
        print "create dir %s"%rcEnv.pathetc
        os.makedirs(rcEnv.pathetc)
    s = []
    for name in os.listdir(rcEnv.pathetc):
        if not is_service(os.path.join(rcEnv.pathetc, name)):
            continue
        s.append(name)
    return s

def build_services(status=None, svcnames=[],
                   onlyprimary=False, onlysecondary=False):
    """returns a list of all services of status matching the specified status.
    If no status is specified, returns all services
    """
    services = {}
    for name in list_services():
        if len(svcnames) > 0 and name not in svcnames:
            continue
        fix_default_section([name])
        try:
            svc = build(name)
        except (ex.excError, ex.excInitError, ex.excAbortAction):
            continue
        except:
            import traceback
            traceback.print_exc()
            continue
        if svc is None :
            continue
        if status is not None and svc.status() != status:
            continue
        if onlyprimary and rcEnv.nodename not in svc.autostart_node:
            continue
        if onlysecondary and rcEnv.nodename in svc.autostart_node:
            continue
        services[svc.svcname] = svc
    return [ s for n ,s in sorted(services.items()) ]

def toggle_one(svcname, rids=[], disable=True):
    if len(svcname) == 0:
        print >>sys.stderr, "service name must not be empty"
        return 1
    if svcname not in list_services():
        print >>sys.stderr, "service", svcname, "does not exist"
        return 1
    if len(rids) == 0:
        rids = ['DEFAULT']
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    conf = ConfigParser.RawConfigParser()
    conf.read(envfile)
    for rid in rids:
        if rid != 'DEFAULT' and not conf.has_section(rid):
            print >>sys.stderr, "service", svcname, "has not resource", rid
            continue
        conf.set(rid, "disable", disable)
    try:
       f = open(envfile, 'w')
    except:
        print >>sys.stderr, "failed to open", envfile, "for writing"
        return 1

    #
    # if we set DEFAULT.disable = True,
    # we don't want res#n.disable = False
    #
    if len(rids) == 0 and disable:
        for s in conf.sections():
            if conf.has_option(s, "disable") and \
               conf.getboolean(s, "disable") == False:
                conf.remove_option(s, "disable")

    conf.write(f)
    return 0

def disable_one(svcname, rids=[]):
    return toggle_one(svcname, rids, disable=True)

def disable(svcnames, rid=[]):
    fix_default_section(svcnames)
    r = 0
    for svcname in svcnames:
        r |= disable_one(svcname, rid)
    return r

def enable_one(svcname, rids=[]):
    return toggle_one(svcname, rids, disable=False)

def enable(svcnames, rid=[]):
    fix_default_section(svcnames)
    r = 0
    for svcname in svcnames:
        r |= enable_one(svcname, rid)
    return r

def delete_one(svcname, rids=[]):
    if len(svcname) == 0:
        print >>sys.stderr, "service name must not be empty"
        return 1
    if svcname not in list_services():
        print >>sys.stderr, "service", svcname, "does not exist"
        return 0
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    conf = ConfigParser.RawConfigParser()
    conf.read(envfile)
    for rid in rids:
        if not conf.has_section(rid):
            print >>sys.stderr, "service", svcname, "has not resource", rid
            continue
        conf.remove_section(rid)
    try:
       f = open(envfile, 'w')
    except:
        print >>sys.stderr, "failed to open", envfile, "for writing"
        return 1
    conf.write(f)
    return 0

def delete(svcnames, rid=[]):
    fix_default_section(svcnames)
    if len(rid) == 0:
        print "no resource flagged for deletion"
        return 0
    r = 0
    for svcname in svcnames:
        r |= delete_one(svcname, rid)
    return r

def create(svcname, resources=[], interactive=False, provision=False):
    if not isinstance(svcname, list):
        print >>sys.stderr, "ouch, svcname should be a list object"
        return 1
    if len(svcname) != 1:
        print >>sys.stderr, "you must specify a single service name with the 'create' action"
        return 1
    svcname = svcname[0]
    if len(svcname) == 0:
        print >>sys.stderr, "service name must not be empty"
        return 1
    if svcname in list_services():
        print >>sys.stderr, "service", svcname, "already exists"
        return 1
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    if os.path.exists(envfile):
        print >>sys.stderr, envfile, "already exists"
        return 1
    try:
       f = open(envfile, 'w')
    except:
        print >>sys.stderr, "failed to open", envfile, "for writing"
        return 1

    defaults = {}
    sections = {}
    rtypes = {}

    import json
    for r in resources:
        try:
            d = json.loads(r)
        except:
            print >>sys.stderr, "can not parse resource:", r
            return 1
        if 'rid' in d:
            section = d['rid']
            if '#' not in section:
                print >>sys.stderr, section, "must be formatted as 'rtype#n'"
                return 1
            l = section.split('#')
            if len(l) != 2:
                print >>sys.stderr, section, "must be formatted as 'rtype#n'"
                return 1
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
        elif 'rtype' in d:
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
            defaults.update(d)

    from svcDict import KeyDict, MissKeyNoDefault, KeyInvalidValue
    try:
        keys = KeyDict(provision=provision)
        defaults.update(keys.update('DEFAULT', defaults))
        for section, d in sections.items():
            sections[section].update(keys.update(section, d))
    except (MissKeyNoDefault, KeyInvalidValue):
        if not interactive:
            return 1

    try:
        if interactive:
            defaults, sections = keys.form(defaults, sections)
    except KeyboardInterrupt:
        sys.stderr.write("Abort\n")
        return 1

    conf = ConfigParser.RawConfigParser(defaults)
    for section, d in sections.items():
        conf.add_section(section)
        for key, val in d.items():
            if key == 'rtype':
                continue
            conf.set(section, key, val)

    conf.write(f)

    os.chdir(rcEnv.pathetc)
    if os.path.exists(svcname) and not os.path.islink(svcname):
        os.unlink(svcname)
    if not os.path.exists(svcname):
        os.symlink(os.path.join('..', 'bin', 'svcmgr'), svcname)
    initdir = svcname+'.dir'
    if not os.path.exists(initdir):
        os.makedirs(initdir)
    if not os.path.islink(svcname+'.d') and os.path.exists(svcname+'.d'):
        os.unlink(svcname+'.d')
    if not os.path.exists(svcname+'.d'):
        os.symlink(initdir, svcname+'.d')

def update(svcname, resources=[], interactive=False, provision=False):
    fix_default_section(svcname)
    if not isinstance(svcname, list):
        print >>sys.stderr, "ouch, svcname should be a list object"
        return 1
    if len(svcname) != 1:
        print >>sys.stderr, "you must specify a single service name with the 'create' action"
        return 1
    svcname = svcname[0]
    if len(svcname) == 0:
        print >>sys.stderr, "service name must not be empty"
        return 1
    if svcname not in list_services():
        print >>sys.stderr, "service", svcname, "does not exist"
        return 1
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    sections = {}
    rtypes = {}
    conf = ConfigParser.RawConfigParser()
    conf.read(envfile)
    defaults = conf.defaults()
    for section in conf.sections():
        sections[section] = {}
        for o, v in conf.items(section):
            if o in defaults.keys() + ['rtype']:
                continue
            sections[section][o] = v

    import json
    for r in resources:
        try:
            d = json.loads(r)
        except:
            print >>sys.stderr, "can not parse resource:", r
            return 1
        if 'rid' in d:
            section = d['rid']
            if '#' not in section:
                print >>sys.stderr, section, "must be formatted as 'rtype#n'"
                return 1
            l = section.split('#')
            if len(l) != 2:
                print >>sys.stderr, section, "must be formatted as 'rtype#n'"
                return 1
            rtype = l[0]
            if rtype in rtypes:
                rtypes[rtype] += 1
            else:
                rtypes[rtype] = 0
            del(d['rid'])
            if section in sections:
                sections[section].update(d)
            else:
                sections[section] = d
        elif 'rtype' in d:
            print >>sys.stderr, section, "'rtype' can not be used with the 'update' command"
            return 1
        else:
            defaults.update(d)

    conf = ConfigParser.RawConfigParser(defaults)
    for section, d in sections.items():
        conf.add_section(section)
        for key, val in d.items():
            conf.set(section, key, val)

    try:
        f = open(envfile, 'w')
    except:
        print >>sys.stderr, "failed to open", envfile, "for writing"
        return 1

    conf.write(f)

    os.chdir(rcEnv.pathetc)
    if not os.path.islink(svcname):
        os.unlink(svcname)
    if not os.path.exists(svcname):
        os.symlink(os.path.join('..', 'bin', 'svcmgr'), svcname)
    initdir = svcname+'.dir'
    if not os.path.exists(initdir):
        os.makedirs(initdir)
    if not os.path.islink(svcname+'.d'):
        os.unlink(svcname+'.d')
    if not os.path.exists(svcname+'.d'):
        os.symlink(initdir, svcname+'.d')

def _fix_default_section(svcname):
    """ [default] section is not returned by ConfigParser.defaults()
        [DEFAULT] is. Just replace when this occurs.
    """
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    if not os.path.exists(envfile):
        # nothing to fix
        return
    try:
        f = open(envfile, 'r')
    except:
        print >>sys.stderr, "failed to open", envfile, "for reading"
        return 1
    found = False
    lines = []
    for line in f.readlines():
        if line.startswith('[default]'):
            line = '[DEFAULT]\n'
            found = True
        lines.append(line)
    f.close()
    if found:
        try:
            f = open(envfile, 'w')
        except:
            print >>sys.stderr, "failed to open", envfile, "for writing"
            return 1
        f.write(''.join(lines))
        f.close()

def fix_default_section(svcnames):
    for svcname in svcnames:
        _fix_default_section(svcname)
