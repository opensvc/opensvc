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
from __future__ import print_function
import os
import sys
import logging
import re
import socket
import glob

from rcGlobalEnv import *
from rcNode import discover_node
from rcUtilities import *
import rcLogger
import resSyncRsync
import rcExceptions as ex
import platform

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

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

os.environ['LANG'] = 'C'
os.environ['PATH'] += ':/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'

def conf_get(svc, conf, s, o, t, scope=False, impersonate=None):
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
    if type(svc) != dict:
        d = {
         'nodes': svc.nodes,
         'drpnodes': svc.drpnodes,
         'encapnodes': svc.encapnodes,
        }
    else:
        d = svc

    if impersonate is None:
        nodename = rcEnv.nodename
    else:
        nodename = impersonate

    if conf.has_option(s, o+"@"+nodename):
        return f(s, o+"@"+nodename)
    elif conf.has_option(s, o+"@nodes") and \
         nodename in d['nodes']:
        return f(s, o+"@nodes")
    elif conf.has_option(s, o+"@drpnodes") and \
         nodename in d['drpnodes']:
        return f(s, o+"@drpnodes")
    elif conf.has_option(s, o+"@encapnodes") and \
         nodename in d['encapnodes']:
        return f(s, o+"@encapnodes")
    elif conf.has_option(s, o):
        return f(s, o)
    else:
        raise ex.OptNotFound

def conf_get_string(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'string', scope=False)

def conf_get_string_scope(svc, conf, s, o, impersonate=None):
    return conf_get(svc, conf, s, o, 'string', scope=True, impersonate=impersonate)

def conf_get_boolean(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'boolean', scope=False)

def conf_get_boolean_scope(svc, conf, s, o, impersonate=None):
    return conf_get(svc, conf, s, o, 'boolean', scope=True, impersonate=impersonate)

def conf_get_int(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'integer', scope=False)

def conf_get_int_scope(svc, conf, s, o, impersonate=None):
    return conf_get(svc, conf, s, o, 'integer', scope=True, impersonate=impersonate)

def svcmode_mod_name(svcmode=''):
    """Returns (moduleName, serviceClassName) implementing the class for
    a given service mode. For example:
    hosted => ('svcHosted', 'SvcHosted')
    """
    if svcmode == 'hosted':
        return ('svcHosted', 'SvcHosted')
    elif svcmode == 'sg':
        return ('svcSg', 'SvcSg')
    elif svcmode == 'rhcs':
        return ('svcRhcs', 'SvcRhcs')
    elif svcmode == 'vcs':
        return ('svcVcs', 'SvcVcs')
    raise ex.excError("unknown service mode: %s"%svcmode)

def get_tags(conf, section, svc):
    try:
        s = conf_get_string_scope(svc, conf, section, 'tags')
    except ex.OptNotFound:
        s = ""
    return set(s.split())

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

def get_subset(conf, section, svc):
    if not conf.has_section(section):
        return
    try:
        return conf_get_string_scope(svc, conf, section, 'subset')
    except ex.OptNotFound:
        return
    return

def get_restart(conf, section, svc):
    if not conf.has_section(section):
        if conf.has_option('DEFAULT', 'restart'):
            try:
                return conf_get_int_scope(svc, conf, section, 'restart')
            except ex.OptNotFound:
                return 0
        else:
            return 0
    try:
        return conf_get_int_scope(svc, conf, section, 'restart')
    except ex.OptNotFound:
        return 0
    return 0

def get_disabled(conf, section, svc):
    if conf.has_option('DEFAULT', 'disable'):
        svc_disable = conf.getboolean("DEFAULT", "disable")
    else:
        svc_disable = False

    if svc_disable is True:
        return True

    if section == "":
        return svc_disable

    try:
        r = conf_get_boolean_scope(svc, conf, section, 'disable')
        return r
    except ex.OptNotFound:
        pass
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

    try:
        pa = conf_get_boolean_scope(svc, conf, resource.rid, 'no_preempt_abort')
    except ex.OptNotFound:
        defaults = conf.defaults()
        if 'no_preempt_abort' in defaults:
            pa = bool(defaults['no_preempt_abort'])
        else:
            pa = False

    kwargs = {}
    kwargs['rid'] = resource.rid
    kwargs['tags'] = resource.tags
    kwargs['disks'] = resource.disklist()
    kwargs['no_preempt_abort'] = pa
    kwargs['disabled'] = resource.is_disabled()
    kwargs['optional'] = resource.is_optional()

    r = sr.ScsiReserv(**kwargs)
    svc += r

def add_triggers(svc, resource, conf, section):
    triggers = ['pre_stop', 'pre_start', 'pre_syncnodes', 'pre_syncdrp',
                'post_stop', 'post_start', 'post_syncnodes', 'post_syncdrp',
                'post_syncresync', 'pre_syncresync']
    for trigger in triggers:
        try:
            s = conf_get_string_scope(svc, conf, resource.rid, trigger)
        except ex.OptNotFound:
            continue
        setattr(resource, trigger, s.split())

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

def add_resources(restype, svc, conf):
    for s in conf.sections():
        if s != 'app' and s != restype and re.match(restype+'#[0-9]', s, re.I) is None:
            continue
        if svc.encap and 'encap' not in get_tags(conf, s, svc):
            continue
        if not svc.encap and 'encap' in get_tags(conf, s, svc):
            svc.has_encap_resources = True
            continue
        globals()['add_'+restype](svc, conf, s)
 
def add_ip(svc, conf, s):
    """Parse the configuration file and add an ip object for each [ip#n]
    section. Ip objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['ipName'] = conf_get_string_scope(svc, conf, s, 'ipname')
    except ex.OptNotFound:
        svc.log.error("nor ipname and ipname@%s defined in config file section %s"%(rcEnv.nodename, s))
        return

    try:
        kwargs['ipDev'] = conf_get_string_scope(svc, conf, s, 'ipdev')
    except ex.OptNotFound:
        svc.log.debug('ipdev not found in ip section %s'%s)
        return

    try:
        kwargs['mask'] = conf_get_string_scope(svc, conf, s, 'netmask')
    except ex.OptNotFound:
        pass

    try:
        kwargs['gateway'] = conf_get_string_scope(svc, conf, s, 'gateway')
    except ex.OptNotFound:
        pass

    try:
        kwargs['zone'] = conf_get_string_scope(svc, conf, s, 'zone')
    except ex.OptNotFound:
        pass

    try:
        rtype = conf_get_string_scope(svc, conf, s, 'type')
    except ex.OptNotFound:
        rtype = None

    if rtype == "crossbow":
        if 'zone' in kwargs:
            svc.log.error("'zone' and 'type=crossbow' are incompatible in section %s"%s)
            return
        ip = __import__('resIp'+'Crossbow')
    elif 'zone' in kwargs:
        ip = __import__('resIp'+'Zone')
    else:
        ip = __import__('resIp'+rcEnv.sysname)

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    r = ip.Ip(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_drbd(svc, conf, s):
    """Parse the configuration file and add a drbd object for each [drbd#n]
    section. Drbd objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['res'] = conf_get_string(svc, conf, s, 'res')
    except ex.OptNotFound:
        svc.log.error("res must be set in section %s"%s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    drbd = __import__('resDrbd')
    r = drbd.Drbd(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_vdisk(svc, conf, s):
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
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    vdisk = __import__('resVdisk')
    r = vdisk.Vdisk(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_stonith(svc, conf, s):
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
    elif _type in ('Callout'):
        try:
            kwargs['cmd'] = conf_get_string_scope(svc, conf, s, 'cmd')
        except ex.OptNotFound:
            pass
    
        if 'cmd' not in kwargs:
            svc.log.error("cmd must be set in section %s"%s)
            return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)

    st = __import__('resStonith'+_type)
    try:
        st = __import__('resStonith'+_type)
    except ImportError:
        svc.log.error("resStonith%s is not implemented"%_type)
        return

    r = st.Stonith(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_hb(svc, conf, s):
    if rcEnv.nodename in svc.drpnodes:
        # no heartbeat on DRP nodes
        return

    kwargs = {}

    try:
        hbtype = conf_get_string(svc, conf, s, 'type').lower()
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%s)
        return

    try:
        kwargs['name'] = conf_get_string(svc, conf, s, 'name')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)

    if hbtype == 'openha':
        hbtype = 'OpenHA'
    elif hbtype == 'linuxha':
        hbtype = 'LinuxHA'

    try:
        hb = __import__('resHb'+hbtype)
    except ImportError:
        svc.log.error("resHb%s is not implemented"%hbtype)
        return

    r = hb.Hb(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_loop(svc, conf, s):
    """Parse the configuration file and add a loop object for each [loop#n]
    section. Loop objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['loopFile'] = conf_get_string_scope(svc, conf, s, 'file')
    except ex.OptNotFound:
        svc.log.error("file must be set in section %s"%s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    try:
        loop = __import__('resLoop'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resLoop%s is not implemented"%rcEnv.sysname)
        return

    r = loop.Loop(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_vg(svc, conf, s):
    """Parse the configuration file and add a vg object for each [vg#n]
    section. Vg objects are stored in a list in the service object.
    """
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
            kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
        except ex.OptNotFound:
            pass
        try:
            kwargs['group'] = conf_get_string_scope(svc, conf, s, 'user')
        except ex.OptNotFound:
            pass
        try:
            kwargs['perm'] = conf_get_string_scope(svc, conf, s, 'perm')
        except ex.OptNotFound:
            pass
        try:
            kwargs['dummy'] = conf_get_boolean_scope(svc, conf, s, 'dummy')
        except ex.OptNotFound:
            pass
    elif vgtype == 'Gandi':
        try:
            kwargs['cloud_id'] = conf_get_string_scope(svc, conf, s, 'cloud_id')
        except ex.OptNotFound:
            svc.log.error("cloud_id must be set in section %s"%s)
            return
        try:
            kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
        except ex.OptNotFound:
            svc.log.error("name must be set in section %s"%s)
            return
        try:
            kwargs['node'] = conf_get_string_scope(svc, conf, s, 'node')
        except ex.OptNotFound:
            pass
        try:
            kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
        except ex.OptNotFound:
            pass
        try:
            kwargs['group'] = conf_get_string_scope(svc, conf, s, 'user')
        except ex.OptNotFound:
            pass
        try:
            kwargs['perm'] = conf_get_string_scope(svc, conf, s, 'perm')
        except ex.OptNotFound:
            pass

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'vgname')
    except ex.OptNotFound:
        if not vgtype.startswith("Raw") and vgtype != "Gandi":
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
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    try:
        vg = __import__('resVg'+vgtype)
    except ImportError:
        svc.log.error("vg type %s is not implemented"%vgtype)
        return

    r = vg.Vg(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_vmdg(svc, conf, s):
    kwargs = {}
    s = 'vmdg'

    try:
        kwargs['container_id'] = conf_get_string_scope(svc, conf, s, 'container_id')
    except ex.OptNotFound:
        svc.log.error("container_id must be set in section %s"%s)
        return

    if not conf.has_section(kwargs['container_id']):
        svc.log.error("%s.container_id points to an invalid section"%kwargs['container_id'])
        return

    try:
        container_type = conf_get_string_scope(svc, conf, kwargs['container_id'], 'type')
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%kwargs['container_id'])
        return

    if container_type == 'ldom':
        vg = __import__('resVgLdom')
    else:
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['name'] = s
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = vg.Vg(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_pool(svc, conf, s):
    """Parse the configuration file and add a pool object for each [pool#n]
    section. Pools objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'poolname')
    except ex.OptNotFound:
        svc.log.error("poolname must be set in section %s"%s)
        return

    try:
        zone = conf_get_string_scope(svc, conf, s, 'zone')
    except ex.OptNotFound:
        zone = None

    pool = __import__('resVgZfs')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = pool.Pool(**kwargs)

    if zone is not None:
        r.tags.add('zone')
        r.tags.add(zone)

    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_share(svc, conf, s):
    try:
        _type = conf_get_string_scope(svc, conf, s, 'type')
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%s)
        return

    fname = 'add_share_'+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s)) 
    globals()[fname](svc, conf, s)

def add_share_nfs(svc, conf, s):
    kwargs = {}

    try:
        kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
    except ex.OptNotFound:
        svc.log.error("path must be set in section %s"%s)
        return

    try:
        kwargs['opts'] = conf_get_string_scope(svc, conf, s, 'opts')
    except ex.OptNotFound:
        svc.log.error("opts must be set in section %s"%s)
        return

    try:
        share = __import__('resShareNfs'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resShareNfs%s is not implemented"%rcEnv.sysname)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = share.Share(**kwargs)

    add_triggers(svc, r, conf, s)
    svc += r

def add_fs(svc, conf, s):
    """Parse the configuration file and add a fs object for each [fs#n]
    section. Fs objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['device'] = conf_get_string_scope(svc, conf, s, 'dev')
    except ex.OptNotFound:
        svc.log.error("dev must be set in section %s"%s)
        return

    try:
        kwargs['mountPoint'] = conf_get_string_scope(svc, conf, s, 'mnt')
    except ex.OptNotFound:
        svc.log.error("mnt must be set in section %s"%s)
        return

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

    try:
        zone = conf_get_string_scope(svc, conf, s, 'zone')
    except:
        zone = None

    if zone is not None:
        zp = None
        for r in svc.get_resources("container.zone"):
            if r.name == zone:
                zp = r.zonepath
                break
        if zp is None:
            svc.log.error("zone %s, referenced in %s, not found"%(zone, s))
            raise ex.excError()
        kwargs['mountPoint'] = os.path.realpath(zp+'/root/'+kwargs['mountPoint'])

    try:
        mount = __import__('resMount'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resMount%s is not implemented"%rcEnv.sysname)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = mount.Mount(**kwargs)

    if zone is not None:
        r.tags.add(zone)
        r.tags.add('zone')

    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_esx(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerEsx')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Esx(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_hpvm(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerHpVm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.HpVm(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_ldom(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerLdom')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Ldom(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_vbox(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerVbox')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Vbox(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_xen(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerXen')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Xen(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_zone(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerZone')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Zone(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)



def add_containers_vcloud(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cloud_id'] = conf_get_string_scope(svc, conf, s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return

    try:
        kwargs['vapp'] = conf_get_string_scope(svc, conf, s, 'vapp')
    except ex.OptNotFound:
        svc.log.error("vapp must be set in section %s"%s)
        return

    m = __import__('resContainerVcloud')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.CloudVm(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_openstack(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cloud_id'] = conf_get_string_scope(svc, conf, s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return

    try:
        kwargs['size'] = conf_get_string_scope(svc, conf, s, 'size')
    except ex.OptNotFound:
        svc.log.error("size must be set in section %s"%s)
        return

    try:
        kwargs['key_name'] = conf_get_string_scope(svc, conf, s, 'key_name')
    except ex.OptNotFound:
        svc.log.error("key_name must be set in section %s"%s)
        return

    try:
        kwargs['shared_ip_group'] = conf_get_string_scope(svc, conf, s, 'shared_ip_group')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerOpenstack')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.CloudVm(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_vz(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerVz')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Vz(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_kvm(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerKvm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Kvm(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_srp(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerSrp')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Srp(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_lxc(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cf'] = conf_get_string_scope(svc, conf, s, 'cf')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerLxc')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Lxc(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_ovm(svc, conf, s):
    kwargs = {}

    try:
        kwargs['uuid'] = conf_get_string_scope(svc, conf, s, 'uuid')
    except ex.OptNotFound:
        svc.log.error("uuid must be set in section %s"%s)
        return

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerOvm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Ovm(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_jail(svc, conf, s):
    kwargs = {}

    try:
        kwargs['jailroot'] = conf_get_string_scope(svc, conf, s, 'jailroot')
    except ex.OptNotFound:
        svc.log.error("jailroot must be set in section %s"%s)
        return

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['ips'] = conf_get_string_scope(svc, conf, s, 'ips').split()
    except ex.OptNotFound:
        pass

    try:
        kwargs['ip6s'] = conf_get_string_scope(svc, conf, s, 'ip6s').split()
    except ex.OptNotFound:
        pass

    m = __import__('resContainerJail')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Jail(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers(svc, conf):
    for t in rcEnv.vt_supported:
        add_containers_resources(t, svc, conf)

def add_containers_resources(subtype, svc, conf):
    add_sub_resources('container', subtype, svc, conf)

def add_mandatory_syncs(svc, conf):
    """Mandatory files to sync:
    1/ to all nodes: service definition
    2/ to drpnodes: system files to replace on the drpnode in case of startdrp
    """

    """1
    """
    if len(svc.nodes|svc.drpnodes) > 1:
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
        if conf.has_option(kwargs['rid'], 'options'):
            kwargs['options'] += conf.get(kwargs['rid'], 'options').split()
        kwargs['target'] = targethash
        kwargs['internal'] = True
        kwargs['disabled'] = get_disabled(conf, kwargs['rid'], svc)
        kwargs['optional'] = get_optional(conf, kwargs['rid'], svc)
        kwargs.update(get_sync_args(conf, 'sync', svc))
        r = resSyncRsync.Rsync(**kwargs)
        svc += r

    """2
    """
    if len(svc.drpnodes) == 0:
        return

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
        if conf.has_option(kwargs['rid'], 'options'):
            kwargs['options'] += conf.get(kwargs['rid'], 'options').split()
        kwargs['target'] = targethash
        kwargs['internal'] = True
        kwargs['disabled'] = get_disabled(conf, kwargs['rid'], svc)
        kwargs['optional'] = get_optional(conf, kwargs['rid'], svc)
        kwargs.update(get_sync_args(conf, 'sync', svc))
        r = resSyncRsync.Rsync(**kwargs)
        svc += r

def add_syncs_resources(subtype, svc, conf):
    add_sub_resources('sync', subtype, svc, conf, default_subtype="rsync")

def add_sub_resources(restype, subtype, svc, conf, default_subtype=None):
    for s in conf.sections():
        if re.match(restype+'#i[0-9]', s, re.I) is None and \
           re.match(restype+'#[0-9]', s, re.I) is None:
            continue
        if svc.encap and 'encap' not in get_tags(conf, s, svc):
            continue
        if not svc.encap and 'encap' in get_tags(conf, s, svc):
            svc.has_encap_resources = True
            continue
        try:
            res_subtype = conf_get_string_scope(svc, conf, s, "type")
        except ex.OptNotFound:
            res_subtype = default_subtype

        if subtype != res_subtype:
            continue

        globals()['add_'+restype+'s_'+subtype](svc, conf, s)

def add_syncs(svc, conf):
    add_syncs_resources('rsync', svc, conf)
    add_syncs_resources('netapp', svc, conf)
    add_syncs_resources('nexenta', svc, conf)
    add_syncs_resources('symclone', svc, conf)
    add_syncs_resources('symsrdfs', svc, conf)
    add_syncs_resources('hp3par', svc, conf)
    add_syncs_resources('ibmdssnap', svc, conf)
    add_syncs_resources('evasnap', svc, conf)
    add_syncs_resources('necismsnap', svc, conf)
    add_syncs_resources('dcssnap', svc, conf)
    add_syncs_resources('dcsckpt', svc, conf)
    add_syncs_resources('dds', svc, conf)
    add_syncs_resources('zfs', svc, conf)
    add_syncs_resources('btrfs', svc, conf)
    add_mandatory_syncs(svc, conf)

def add_syncs_btrfs(svc, conf, s):
    kwargs = {}

    try:
        kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    try:
        kwargs['dst'] = conf_get_string_scope(svc, conf, s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

    try:
        kwargs['recursive'] = conf_get_boolean(svc, conf, s, 'recursive')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    btrfs = __import__('resSyncBtrfs')
    r = btrfs.SyncBtrfs(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_zfs(svc, conf, s):
    kwargs = {}

    try:
        kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    try:
        kwargs['dst'] = conf_get_string_scope(svc, conf, s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

    try:
        kwargs['recursive'] = conf_get_boolean(svc, conf, s, 'recursive')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    zfs = __import__('resSyncZfs')
    r = zfs.SyncZfs(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_dds(svc, conf, s):
    kwargs = {}

    try:
        kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    dsts = {}
    for node in svc.nodes | svc.drpnodes:
        dst = conf_get_string_scope(svc, conf, s, 'dst', impersonate=node)
        dsts[node] = dst

    if len(dsts) == 0:
        for node in svc.nodes | svc.drpnodes:
            dsts[node] = kwargs['src']
    
    kwargs['dsts'] = dsts

    try:
        kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

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
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    dds = __import__('resSyncDds')
    r = dds.syncDds(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_dcsckpt(svc, conf, s):
    kwargs = {}

    try:
        kwargs['dcs'] = set(conf_get_string_scope(svc, conf, s, 'dcs').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'dcs' set" % s)
        return

    try:
        kwargs['manager'] = set(conf_get_string_scope(svc, conf, s, 'manager').split())
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
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncDcsCkpt'+rcEnv.sysname)
    except:
        sc = __import__('resSyncDcsCkpt')
    r = sc.syncDcsCkpt(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_dcssnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['dcs'] = set(conf_get_string(svc, conf, s, 'dcs').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'dcs' set" % s)
        return

    try:
        kwargs['manager'] = set(conf_get_string(svc, conf, s, 'manager').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'manager' set" % s)
        return

    try:
        kwargs['snapname'] = set(conf_get_string(svc, conf, s, 'snapname').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'snapname' set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncDcsSnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncDcsSnap')
    r = sc.syncDcsSnap(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_necismsnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['array'] = conf_get_string_scope(svc, conf, s, 'array')
    except ex.OptNotFound:
        pass

    try:
        kwargs['devs'] = conf_get_string_scope(svc, conf, s, 'devs')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have devs set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncNecIsmSnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncNecIsmSnap')
    r = sc.syncNecIsmSnap(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_evasnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['eva_name'] = conf_get_string(svc, conf, s, 'eva_name')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have eva_name set" % s)
        return

    try:
        kwargs['snap_name'] = conf_get_string(svc, conf, s, 'snap_name')
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
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncEvasnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncEvasnap')
    r = sc.syncEvasnap(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_hp3par(svc, conf, s):
    kwargs = {}

    try:
        kwargs['mode'] = conf_get_string_scope(svc, conf, s, 'mode')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have mode set" % s)
        return

    try:
        kwargs['array'] = conf_get_string_scope(svc, conf, s, 'array')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have array set" % s)
        return

    rcg_names = {}
    for node in svc.nodes | svc.drpnodes:
        array = conf_get_string_scope(svc, conf, s, 'array', impersonate=node)
        rcg = conf_get_string_scope(svc, conf, s, 'rcg', impersonate=node)
        rcg_names[array] = rcg

    if len(rcg_names) == 0:
        svc.log.error("config file section %s must have rcg set" % s)
        return

    kwargs['rcg_names'] = rcg_names

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncHp3par'+rcEnv.sysname)
    except:
        sc = __import__('resSyncHp3par')
    r = sc.syncHp3par(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_symsrdfs(svc, conf, s):
    kwargs = {}

    try:
        kwargs['symdg'] = conf_get_string(svc, conf, s, 'symdg')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have symdg set" % s)
        return

    try:
        kwargs['rdfg'] = conf_get_int(svc, conf, s, 'rdfg')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have rdfg number set" % s)
        return


    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncSymSrdfS'+rcEnv.sysname)
    except:
        sc = __import__('resSyncSymSrdfS')
    r = sc.syncSymSrdfS(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r


def add_syncs_symclone(svc, conf, s):
    kwargs = {}

    try:
        kwargs['symdg'] = conf_get_string(svc, conf, s, 'symdg')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have symdg set" % s)
        return

    try:
        kwargs['symdevs'] = conf_get_string_scope(svc, conf, s, 'symdevs').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have symdevs set" % s)
        return

    try:
        kwargs['precopy_timeout'] = conf_get_int(svc, conf, s, 'precopy_timeout')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncSymclone'+rcEnv.sysname)
    except:
        sc = __import__('resSyncSymclone')
    r = sc.syncSymclone(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_ibmdssnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['pairs'] = conf_get_string(svc, conf, s, 'pairs').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have pairs set" % s)
        return

    try:
        kwargs['array'] = conf_get_string(svc, conf, s, 'array')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have array set" % s)
        return

    try:
        kwargs['bgcopy'] = conf_get_boolean(svc, conf, s, 'bgcopy')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have bgcopy set" % s)
        return

    try:
        kwargs['recording'] = conf_get_boolean(svc, conf, s, 'recording')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have recording set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        m = __import__('resSyncIbmdsSnap'+rcEnv.sysname)
    except:
        m = __import__('resSyncIbmdsSnap')
    r = m.syncIbmdsSnap(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_nexenta(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'name' set" % s)
        return

    try:
        kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have path set" % s)
        return

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
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))

    import resSyncNexenta
    r = resSyncNexenta.syncNexenta(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_netapp(svc, conf, s):
    kwargs = {}

    try:
        kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have path set" % s)
        return

    try:
        kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
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
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))

    import resSyncNetapp
    r = resSyncNetapp.syncNetapp(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_syncs_rsync(svc, conf, s):
    options = []
    kwargs = {}
    kwargs['src'] = []
    try:
        _s = conf_get_string_scope(svc, conf, s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    for src in _s.split():
        kwargs['src'] += glob.glob(src)

    try:
        kwargs['dst'] = conf_get_string_scope(svc, conf, s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['dstfs'] = conf_get_string_scope(svc, conf, s, 'dstfs')
    except ex.OptNotFound:
        pass

    try:
        _s = conf_get_string_scope(svc, conf, s, 'options')
        options += _s.split()
    except ex.OptNotFound:
        pass

    try:
        # for backward compat (use options keyword now)
        _s = conf_get_string_scope(svc, conf, s, 'exclude')
        options += _s.split()
    except ex.OptNotFound:
        pass

    kwargs['options'] = options

    try:
        kwargs['snap'] = conf_get_boolean_scope(svc, conf, s, 'snap')
    except ex.OptNotFound:
        pass

    try:
        _s = conf_get_string_scope(svc, conf, s, 'target')
        target = _s.split()
    except ex.OptNotFound:
        target = []

    try:
        kwargs['bwlimit'] = conf_get_int_scope(svc, conf, s, 'bwlimit')
    except ex.OptNotFound:
        pass

    targethash = {}
    if 'nodes' in target: targethash['nodes'] = svc.nodes
    if 'drpnodes' in target: targethash['drpnodes'] = svc.drpnodes
    kwargs['target'] = targethash
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))

    r = resSyncRsync.Rsync(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_app(svc, conf, s):
    resApp = ximport('resApp')
    kwargs = {}

    try:
        kwargs['script'] = conf_get_string_scope(svc, conf, s, 'script')
    except ex.OptNotFound:
        svc.log.error("'script' is not defined in config file section %s"%s)
        return

    try:
        kwargs['start'] = conf_get_int_scope(svc, conf, s, 'start')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'start'))
        return

    try:
        kwargs['stop'] = conf_get_int_scope(svc, conf, s, 'stop')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'stop'))
        return

    try:
        kwargs['check'] = conf_get_int_scope(svc, conf, s, 'check')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'check'))
        return

    try:
        kwargs['info'] = conf_get_int_scope(svc, conf, s, 'info')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'info'))
        return

    try:
        kwargs['run_as'] = conf_get_string_scope(svc, conf, s, 'run_as')
    except ex.OptNotFound:
        pass

    try:
        kwargs['timeout'] = conf_get_int_scope(svc, conf, s, 'timeout')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
 
    r = resApp.App(**kwargs)
    add_triggers(svc, r, conf, s)
    svc += r

def add_apps_sysv(svc, conf):
    """
    SysV-like launchers support.
    Translate to app#n resources.
    """
    resApp = ximport('resApp')

    s = "app"
    try:
        initd = conf_get_string_scope(svc, conf, s, 'dir')
    except ex.OptNotFound:
        initd = svc.initd

    disabled = get_disabled(conf, s, svc)
    #optional = get_optional(conf, s, svc)
    monitor = get_monitor(conf, s, svc)
    restart = get_restart(conf, s, svc)

    allocated = []

    def get_next_rid():
        rid_index = 0
        _rid = "app#%d" % rid_index
        while _rid in svc.resources_by_id.keys() + allocated:
            rid_index += 1
            _rid = "app#%d" % rid_index
        allocated.append(_rid)
        return _rid

    def init_app(script):
        d = {
          'script': script,
          'rid': get_next_rid(),
          'info': 50, 
          'optional': True, 
          'disabled': disabled, 
          'monitor': monitor, 
          'restart': restart, 
        }
        return d

    def find_app(script):
        for r in svc.get_resources("app"):
            if r.script.startswith(os.sep):
                rscript = r.script
            else:
                rscript = os.path.join(initd, r.script)
            rscript = os.path.realpath(rscript)
            if rscript == script:
                return r
        return

    def get_seq(s):
        s = s[1:]
        i = 0
        while s[i].isdigit() or i == len(s):
            i += 1
        s = int(s[:i])
        return s

    h = {}
    for f in glob.glob(os.path.join(initd, 'S*')):
        script = os.path.realpath(f)
        if find_app(script) is not None:
            continue
        if script not in h:
            h[script] = init_app(script)
        h[script]['start'] = get_seq(os.path.basename(f))

    for f in glob.glob(os.path.join(initd, 'K*')):
        script = os.path.realpath(f)
        if find_app(script) is not None:
            continue
        if script not in h:
            h[script] = init_app(script)
        h[script]['stop'] = get_seq(os.path.basename(f))

    for f in glob.glob(os.path.join(initd, 'C*')):
        script = os.path.realpath(f)
        if find_app(script) is not None:
            continue
        if script not in h:
            h[script] = init_app(script)
        h[script]['check'] = get_seq(os.path.basename(f))

    if "app" not in svc.type2resSets:
        svc += resApp.RsetApps("app")

    for script, kwargs in h.items():
        r = resApp.App(**kwargs)
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
            svcmode = conf_get_string_scope({}, conf, 'DEFAULT', "mode")

        if "encapnodes" in defaults:
            encapnodes = set(conf_get_string_scope({}, conf, 'DEFAULT', "encapnodes").split())
            encapnodes -= set([''])
            encapnodes = set(map(lambda x: x.lower(), encapnodes))
        else:
            encapnodes = set([])

        d_nodes = {'encapnodes': encapnodes}

        if "nodes" in defaults:
            nodes = set(conf_get_string_scope(d_nodes, conf, 'DEFAULT', "nodes").split())
            nodes -= set([''])
            nodes = set(map(lambda x: x.lower(), nodes))
        else:
            nodes = set([])

        if "drpnodes" in defaults:
            drpnodes = set(conf_get_string_scope(d_nodes, conf, 'DEFAULT', "drpnodes").split())
            drpnodes -= set([''])
            drpnodes = set(map(lambda x: x.lower(), drpnodes))
        else:
            drpnodes = set([])

        if "drpnode" in defaults:
            drpnode = conf_get_string_scope(d_nodes, conf, 'DEFAULT', "drpnode").lower()
            drpnodes |= set([drpnode])
            drpnodes -= set([''])
        else:
            drpnode = ''

        d_nodes['nodes'] = nodes
        d_nodes['drpnodes'] = drpnodes

        kwargs['disabled'] = get_disabled(conf, "", "")

        if "pkg_name" in defaults:
            if svcmode not in ["sg", "rhcs", "vcs"]:
                log.error("can not set 'pkg_name' with '%s' mode in %s env"%(svcmode, name))
                return None
            kwargs['pkg_name'] = defaults["pkg_name"]


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
    # Store useful properties
    #
    svc.logfile = logfile
    svc.conf = svcconf
    svc.initd = svcinitd
    svc.config = conf

    #
    # Setup service properties from config file content
    #
    if not hasattr(svc, "nodes"):
        svc.nodes = nodes
    if not hasattr(svc, "drpnodes"):
        svc.drpnodes = drpnodes
    if not hasattr(svc, "drpnode"):
        svc.drpnode = drpnode

    try:
        svc.encapnodes = set(conf_get_string_scope(svc, conf, 'DEFAULT', 'encapnodes').split())
    except ex.OptNotFound:
        pass

    if rcEnv.nodename in svc.encapnodes:
        svc.encap = True
    else:
        svc.encap = False

    #
    # containerization options
    #
    try:
        svc.containerize = conf_get_boolean_scope(svc, conf, "DEFAULT", 'containerize')
    except ex.OptNotFound:
        svc.containerize = False

    try:
        svc.container_cpus = conf_get_string_scope(svc, conf, "DEFAULT", "container_cpus")
    except ex.OptNotFound:
        pass

    try:
        svc.container_cpu_shares = conf_get_string_scope(svc, conf, "DEFAULT", "container_cpu_shares")
    except ex.OptNotFound:
        pass

    try:
        svc.container_mems = conf_get_string_scope(svc, conf, "DEFAULT", "container_mems")
    except ex.OptNotFound:
        pass

    try:
        svc.container_mem_limit = conf_get_string_scope(svc, conf, "DEFAULT", "container_mem_limit")
    except ex.OptNotFound:
        pass

    try:
        svc.container_vmem_limit = conf_get_string_scope(svc, conf, "DEFAULT", "container_vmem_limit")
    except ex.OptNotFound:
        pass

    try:
        svc.autostart_node = conf_get_string_scope(svc, conf, 'DEFAULT', 'autostart_node').split()
    except ex.OptNotFound:
        svc.autostart_node = []

    try:
        anti_affinity = conf_get_string_scope(svc, conf, 'DEFAULT', 'anti_affinity')
        svc.anti_affinity = set(conf_get_string_scope(svc, conf, 'DEFAULT', 'anti_affinity').split())
    except ex.OptNotFound:
        pass
    
    """ prune not managed service
    """
    if svc.svcmode not in rcEnv.vt_cloud and rcEnv.nodename not in svc.nodes | svc.drpnodes:
        svc.log.error('service %s not managed here' % name)
        del(svc)
        return None

    if not hasattr(svc, "clustertype"):
        try:
            svc.clustertype = conf_get_string_scope(svc, conf, 'DEFAULT', 'cluster_type')
        except ex.OptNotFound:
            svc.clustertype = 'failover'

    if 'flex' in svc.clustertype:
        svc.ha = True
    allowed_clustertype = ['failover', 'flex', 'autoflex']
    if svc.clustertype not in allowed_clustertype:
        svc.log.error("invalid cluster type '%s'. allowed: %s"%(svc.svcname, svc.clustertype, ', '.join(allowed_clustertype)))
        del(svc)
        return None

    try:
        svc.flex_primary = conf_get_string_scope(svc, conf, 'DEFAULT', 'flex_primary')
    except ex.OptNotFound:
        svc.flex_primary = ''

    try:
        svc.flex_min_nodes = conf_get_int_scope(svc, conf, 'DEFAULT', 'flex_min_nodes')
    except ex.OptNotFound:
        svc.flex_min_nodes = 1
    if svc.flex_min_nodes < 0:
        svc.log.error("invalid flex_min_nodes '%d' (<0)."%svc.flex_min_nodes)
        del(svc)
        return None
    nb_nodes = len(svc.autostart_node)
    if nb_nodes == 0:
        nb_nodes = 1
    if nb_nodes > 0 and svc.flex_min_nodes > nb_nodes:
        svc.log.error("invalid flex_min_nodes '%d' (>%d nb of nodes)."%(svc.flex_min_nodes, nb_nodes))
        del(svc)
        return None

    try:
        svc.flex_max_nodes = conf_get_int_scope(svc, conf, 'DEFAULT', 'flex_max_nodes')
    except ex.OptNotFound:
        svc.flex_max_nodes = nb_nodes
    if svc.flex_max_nodes < 0:
        svc.log.error("invalid flex_max_nodes '%d' (<0)."%svc.flex_max_nodes)
        del(svc)
        return None
    if svc.flex_max_nodes < svc.flex_min_nodes:
        svc.log.error("invalid flex_max_nodes '%d' (<flex_min_nodes)."%svc.flex_max_nodes)
        del(svc)
        return None

    try:
        svc.flex_cpu_low_threshold = conf_get_int_scope(svc, conf, 'DEFAULT', 'flex_cpu_low_threshold')
    except ex.OptNotFound:
        svc.flex_cpu_low_threshold = 10
    if svc.flex_cpu_low_threshold < 0:
        svc.log.error("invalid flex_cpu_low_threshold '%d' (<0)."%svc.flex_cpu_low_threshold)
        del(svc)
        return None
    if svc.flex_cpu_low_threshold > 100:
        svc.log.error("invalid flex_cpu_low_threshold '%d' (>100)."%svc.flex_cpu_low_threshold)
        del(svc)
        return None

    try:
        svc.flex_cpu_high_threshold = conf_get_int_scope(svc, conf, 'DEFAULT', 'flex_cpu_high_threshold')
    except ex.OptNotFound:
        svc.flex_cpu_high_threshold = 90
    if svc.flex_cpu_high_threshold < 0:
        svc.log.error("invalid flex_cpu_high_threshold '%d' (<0)."%svc.flex_cpu_high_threshold)
        del(svc)
        return None
    if svc.flex_cpu_high_threshold > 100:
        svc.log.error("invalid flex_cpu_high_threshold '%d' (>100)."%svc.flex_cpu_high_threshold)
        del(svc)
        return None

    if not hasattr(svc, "service_type"):
        if "service_type" in defaults:
            svc.svctype = defaults["service_type"]
        else:
            svc.svctype = ''

    if svc.svctype not in rcEnv.allowed_svctype:
        svc.log.error('service %s type %s is not a known service type (%s)'%(svc.svcname, svc.svctype, ', '.join(rcEnv.allowed_svctype)))
        del(svc)
        return None

    """ prune service whose service type does not match host mode
    """
    if svc.svctype != 'PRD' and rcEnv.host_mode == 'PRD':
        svc.log.error('service %s type %s is not allowed to run on this node (host mode %s)' % (svc.svcname, svc.svctype, rcEnv.host_mode))
        del(svc)
        return None

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

    if "cluster" in defaults:
        svc.clustername = defaults["cluster"]

    #
    # instanciate resources
    #
    try:
        add_containers(svc, conf)
        add_resources('hb', svc, conf)
        add_resources('stonith', svc, conf)
        add_resources('ip', svc, conf)
        add_resources('drbd', svc, conf)
        add_resources('loop', svc, conf)
        add_resources('vdisk', svc, conf)
        add_resources('vg', svc, conf)
        add_resources('vmdg', svc, conf)
        add_resources('pool', svc, conf)
        add_resources('fs', svc, conf)
        add_resources('share', svc, conf)
        add_resources('app', svc, conf)
        add_apps_sysv(svc, conf)
        add_syncs(svc, conf)
    except (ex.excInitError, ex.excError) as e:
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
        print("create dir %s"%rcEnv.pathetc)
        os.makedirs(rcEnv.pathetc)

    if os.name == 'nt':
        s = glob.glob(os.path.join(rcEnv.pathetc, '*.env'))
        s = map(lambda x: os.path.basename(x).replace('.env',''), s)
        return s

    # posix
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
    if type(svcnames) == str:
        svcnames = [svcnames]
    for name in list_services():
        if len(svcnames) > 0 and name not in svcnames:
            continue
        fix_default_section([name])
        try:
            svc = build(name)
        except (ex.excError, ex.excInitError) as e:
            log.error(str(e))
            continue
        except ex.excAbortAction:
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

def delete_one(svcname, rids=[]):
    if len(svcname) == 0:
        print("service name must not be empty", file=sys.stderr)
        return 1
    if svcname not in list_services():
        print("service", svcname, "does not exist", file=sys.stderr)
        return 0
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    conf = ConfigParser.RawConfigParser()
    conf.read(envfile)
    for rid in rids:
        if not conf.has_section(rid):
            print("service", svcname, "has not resource", rid, file=sys.stderr)
            continue
        conf.remove_section(rid)
    try:
       f = open(envfile, 'w')
    except:
        print("failed to open", envfile, "for writing", file=sys.stderr)
        return 1
    conf.write(f)
    return 0

def delete(svcnames, rid=[]):
    fix_default_section(svcnames)
    if len(rid) == 0:
        print("no resource flagged for deletion")
        return 0
    r = 0
    for svcname in svcnames:
        r |= delete_one(svcname, rid)
    return r

def create(svcname, resources=[], interactive=False, provision=False):
    if not isinstance(svcname, list):
        print("ouch, svcname should be a list object", file=sys.stderr)
        return 1
    if len(svcname) != 1:
        print("you must specify a single service name with the 'create' action", file=sys.stderr)
        return 1
    svcname = svcname[0]
    if len(svcname) == 0:
        print("service name must not be empty", file=sys.stderr)
        return 1
    if svcname in list_services():
        print("service", svcname, "already exists", file=sys.stderr)
        return 1
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    if os.path.exists(envfile):
        print(envfile, "already exists", file=sys.stderr)
        return 1
    try:
       f = open(envfile, 'w')
    except:
        print("failed to open", envfile, "for writing", file=sys.stderr)
        return 1

    defaults = {}
    sections = {}
    rtypes = {}

    import json
    for r in resources:
        try:
            d = json.loads(r)
        except:
            print("can not parse resource:", r, file=sys.stderr)
            return 1
        if 'rid' in d:
            section = d['rid']
            if '#' not in section:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return 1
            l = section.split('#')
            if len(l) != 2:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
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

    initdir = svcname+'.dir'
    if not os.path.exists(initdir):
        os.makedirs(initdir)
    fix_app_link(svcname)
    fix_exe_link(os.path.join('..', 'bin', 'svcmgr'), svcname)

def update(svcname, resources=[], interactive=False, provision=False):
    fix_default_section(svcname)
    if not isinstance(svcname, list):
        print("ouch, svcname should be a list object", file=sys.stderr)
        return 1
    if len(svcname) != 1:
        print("you must specify a single service name with the 'create' action", file=sys.stderr)
        return 1
    svcname = svcname[0]
    if len(svcname) == 0:
        print("service name must not be empty", file=sys.stderr)
        return 1
    if svcname not in list_services():
        print("service", svcname, "does not exist", file=sys.stderr)
        return 1
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    sections = {}
    rtypes = {}
    conf = ConfigParser.RawConfigParser()
    conf.read(envfile)
    defaults = conf.defaults()
    for section in conf.sections():
        sections[section] = {}
        l = section.split('#')
        if len(l) == 2:
            rtype = l[0]
            ridx = l[1]
            if rtype not in rtypes:
                rtypes[rtype] = set([])
            rtypes[rtype].add(ridx)
        for o, v in conf.items(section):
            if o in defaults.keys() + ['rtype']:
                continue
            sections[section][o] = v

    import json
    for r in resources:
        try:
            d = json.loads(r)
        except:
            print("can not parse resource:", r, file=sys.stderr)
            return 1
        if 'rid' in d:
            section = d['rid']
            if '#' not in section:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return 1
            l = section.split('#')
            if len(l) != 2:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return 1
            del(d['rid'])
            if section in sections:
                sections[section].update(d)
            else:
                sections[section] = d
        elif 'rtype' in d:
            # new resource allocation, auto-allocated rid index
            if d['rtype'] in rtypes:
                ridx = 1
                while str(ridx) in rtypes[d['rtype']]:
                    ridx += 1
                ridx = str(ridx)
                rtypes[d['rtype']].add(ridx)
            else:
                ridx = '1'
                rtypes[d['rtype']] = set([ridx])
            section = '#'.join((d['rtype'], ridx))
            del(d['rtype'])
            sections[section] = d
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
        print("failed to open", envfile, "for writing", file=sys.stderr)
        return 1

    conf.write(f)

    fix_app_link(svcname)
    fix_exe_link(os.path.join('..', 'bin', 'svcmgr'), svcname)

def fix_app_link(svcname):
    os.chdir(rcEnv.pathetc)
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
    os.chdir(rcEnv.pathetc)
    try:
        p = os.readlink(src)
    except:
        os.symlink(dst, src)
        p = dst
    if p != dst:
        os.unlink(src)
        os.symlink(dst, src)

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
        print("failed to open", envfile, "for reading", file=sys.stderr)
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
            print("failed to open", envfile, "for writing", file=sys.stderr)
            return 1
        f.write(''.join(lines))
        f.close()

def fix_default_section(svcnames):
    for svcname in svcnames:
        _fix_default_section(svcname)
