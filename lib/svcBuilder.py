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
import resSyncNetapp
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
    elif svcmode == 'xen':
        return ('svcXen', 'SvcXen')
    elif svcmode == 'vbox':
        return ('svcVbox', 'SvcVbox')
    raise

def get_tags(conf, section):
    if conf.has_option(section, 'tags'):
        return set(conf.get(section, "tags").split())
    return set([])

def get_optional(conf, section):
    if conf.has_option(section, 'optional'):
        return conf.getboolean(section, "optional")
    return False

def get_disabled(conf, section, svc):
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

def need_scsireserv(resource, conf, section):
    """scsireserv = true can be set globally or in a specific
    resource section
    """
    defaults = conf.defaults()
    if conf.has_option(section, 'scsireserv'):
       if conf.getboolean(section, 'scsireserv') == True:
           return True
       else:
           return False
    elif 'scsireserv' in defaults and \
       bool(defaults['scsireserv']) == True:
           return True
    return False

def add_scsireserv(svc, resource, conf, section):
    if not need_scsireserv(resource, conf, section):
        return
    try:
        sr = __import__('resScsiReserv'+rcEnv.sysname)
    except:
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

def add_ips(svc, conf):
    """Parse the configuration file and add an ip object for each [ip#n]
    section. Ip objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('ip#[0-9]', s, re.I) is None:
            continue
        kwargs = {}
        if conf.has_option(s, "ipname@"+rcEnv.nodename):
            kwargs['ipName'] = conf.get(s, "ipname@"+rcEnv.nodename)
        elif conf.has_option(s, "ipname"):
            kwargs['ipName'] = conf.get(s, "ipname")
        else:
            svc.log.error("nor ipname and ipname@%s defined in config file section %s"%(rcEnv.nodename, s))
            continue
        if conf.has_option(s, "ipdev@"+rcEnv.nodename):
            kwargs['ipDev'] = conf.get(s, "ipdev@"+rcEnv.nodename)
        elif conf.has_option(s, "ipdev"):
            kwargs['ipDev'] = conf.get(s, "ipdev")
        else:
            svc.log.debug('add_ips ipdev not found in ip section %s'%s)
            continue
        if hasattr(svc, "vmname"):
            vmname = svc.vmname
        else:
            vmname = svc.svcname
        if conf.has_option(s, "netmask"):
            kwargs['mask'] = conf.get(s, "netmask")
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
        elif svc.svcmode  == 'xen':
            ip = __import__('resIp'+'Xen')
        elif svc.svcmode  == 'vbox':
            ip = __import__('resIp'+'Vbox')
        else:
            ip = __import__('resIp'+rcEnv.sysname)
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)
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

        if conf.has_option(s, "res"):
            kwargs['res'] = conf.get(s, "res")
        else:
            svc.log.error("res must be set in section %s"%s)
            return

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)
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
        kwargs['optional'] = get_optional(conf, s)
        vdisk = __import__('resVdisk')
        r = vdisk.Vdisk(**kwargs)
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

        if conf.has_option(s, "name"):
            kwargs['name'] = conf.get(s, "name")
        if conf.has_option(s, "type"):
            type = conf.get(s, "type")
        else:
            svc.log.error("type must be set in section %s"%s)
            return

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)
        hb = __import__('resHb'+type)
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

        if conf.has_option(s, "file"):
            kwargs['loopFile'] = conf.get(s, "file")
        else:
            svc.log.error("file must be set in section %s"%s)
            return

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)
        loop = __import__('resLoop'+rcEnv.sysname)
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

        if not conf.has_option(s, "vgname"):
            svc.log.error("vgname must be set in section %s"%s)
            return
        else:
            kwargs['name'] = conf.get(s, "vgname")

        if conf.has_option(s, "dsf"):
            kwargs['dsf'] = conf.getboolean(s, "dsf")

        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)

        if conf.has_option(s, "vgtype") and conf.get(s, "vgtype") == "veritas":
            vg = __import__('resVg'+'Veritas')
        else:
            vg = __import__('resVg'+rcEnv.sysname)
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
    kwargs['optional'] = get_optional(conf, 'vmdg')

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
        if conf.has_option(s, "poolname@"+rcEnv.nodename):
            name = conf.get(s, "poolname@"+rcEnv.nodename)
        else:
            name = conf.get(s, "poolname")
        pool = __import__('resVgZfs')

        kwargs = {}
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['name'] = name
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)

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
        if conf.has_option(s, "dev@"+rcEnv.nodename):
            dev = conf.get(s, "dev@"+rcEnv.nodename)
        elif conf.has_option(s, "dev"):
            dev = conf.get(s, "dev")
        else:
            svc.log.error("nor dev and dev@%s defined in config file section %s"%(rcEnv.nodename, s))
            dev = None
            continue
        mnt = conf.get(s, "mnt")
        if mnt[-1] == '/':
            """ Remove trailing / to not risk losing rsync src trailing /
                upon snap mountpoint substitution.
            """
            mnt = mnt[0:-1]
        try:
            type = conf.get(s, "type")
        except:
            type = ""
        try:
            mnt_opt = conf.get(s, "mnt_opt")
        except:
            mnt_opt = ""
        if svc.svcmode == 'zone':
            try:
                globalfs = conf.getboolean(s, "globalfs")
            except:
                globalfs = False
            if globalfs is False:
                mnt = os.path.realpath(svc.zone.zonepath+'/root/'+mnt)

        mount = __import__('resMount'+rcEnv.sysname)

        kwargs = {}
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['mountPoint'] = mnt
        kwargs['device'] = dev
        kwargs['fsType'] = type
        kwargs['mntOpt'] = mnt_opt
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)

        if conf.has_option(s, 'snap_size'):
            kwargs['snap_size'] = conf.getint(s, 'snap_size')

        r = mount.Mount(**kwargs)
        add_triggers(r, conf, s)
        svc += r
        #add_scsireserv(svc, r, conf, s)

def add_mandatory_syncs(svc):
    """Mandatory files to sync:
    1/ to all nodes: service definition
    2/ to drpnodes: system files to replace on the drpnode in case of startdrp
    """

    """1
    """
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
    r = resSyncRsync.Rsync(rid="sync#i0", src=src, dst=dst,
                           options=['-R']+exclude, target=targethash,
                           internal=True)
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
        src = [ s for s in src if os.path.exists(s) ]
        if len(src) == 0:
            continue
        i += 1
        r = resSyncRsync.Rsync(rid="sync#i"+str(i), src=src, dst=dst,
                           options=['-R']+exclude, target=targethash,
                           internal=True)
        svc += r

def add_syncs(svc, conf):
    add_syncs_rsync(svc, conf)
    add_syncs_netapp(svc, conf)
    add_syncs_symclone(svc, conf)
    add_syncs_dds(svc, conf)
    add_syncs_zfs(svc, conf)

def add_syncs_zfs(svc, conf):
    zfs = __import__('resSyncZfs')
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if conf.has_option(s, 'type') and \
           conf.get(s, 'type') != 'zfs':
            continue

        if not conf.has_option(s, 'type'):
            continue

        kwargs = {}

        if conf.has_option(s, "src@"+rcEnv.nodename):
            src = conf.get(s, "src@"+rcEnv.nodename)
        elif conf.has_option(s, 'src'):
            src = conf.get(s, "src")
        else:
            svc.log.error("config file section %s must have src set" % s)
            return
        kwargs['src'] = src

        if conf.has_option(s, "dst@"+rcEnv.nodename):
            dst = conf.get(s, "dst@"+rcEnv.nodename)
        elif conf.has_option(s, 'dst'):
            dst = conf.get(s, "dst")
        else:
            dst = src
        kwargs['dst'] = dst

        if not conf.has_option(s, 'target'):
            svc.log.error("config file section %s must have target set" % s)
            return
        else:
            kwargs['target'] = conf.get(s, 'target').split()

        if conf.has_option(s, 'recursive'):
            kwargs['recursive'] = conf.getboolean(s, 'recursive')

        defaults = conf.defaults()
        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif 'sync_max_delay' in defaults:
            kwargs['sync_max_delay'] = int(defaults['sync_max_delay'])

        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif 'sync_min_delay' in defaults:
            kwargs['sync_min_delay'] = int(defaults['sync_min_delay'])

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)
        r = zfs.SyncZfs(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_dds(svc, conf):
    dds = __import__('resSyncDds')
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if conf.has_option(s, 'type') and \
           conf.get(s, 'type') != 'dds':
            continue

        if not conf.has_option(s, 'type'):
            continue

        kwargs = {}

        if not conf.has_option(s, 'src'):
            svc.log.error("config file section %s must have src set" % s)
            return
        else:
            kwargs['src'] = conf.get(s, 'src')

        if not conf.has_option(s, 'dst'):
            kwargs['dst'] = conf.get(s, 'src')
        else:
            kwargs['dst'] = conf.get(s, 'dst')

        if not conf.has_option(s, 'target'):
            svc.log.error("config file section %s must have target set" % s)
            return
        else:
            kwargs['target'] = conf.get(s, 'target').split()

        if conf.has_option(s, 'sender'):
            kwargs['sender'] = conf.get(s, 'sender')

        if conf.has_option(s, 'snap_size'):
            kwargs['snap_size'] = conf.getint(s, 'snap_size')

        if conf.has_option(s, 'delta_store'):
            kwargs['delta_store'] = conf.get(s, 'delta_store')

        defaults = conf.defaults()
        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif 'sync_max_delay' in defaults:
            kwargs['sync_max_delay'] = int(defaults['sync_max_delay'])

        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif 'sync_min_delay' in defaults:
            kwargs['sync_min_delay'] = int(defaults['sync_min_delay'])

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)
        r = dds.syncDds(**kwargs)
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

        if conf.has_option(s, 'type') and \
           conf.get(s, 'type') != 'symclone':
            continue

        if not conf.has_option(s, 'type'):
            continue

        symdevs = []
        kwargs = {}

        if not conf.has_option(s, 'symdg'):
            svc.log.error("config file section %s must have symdg set" % s)
            return
        else:
            kwargs['symdg'] = conf.get(s, 'symdg')

        defaults = conf.defaults()
        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif 'sync_max_delay' in defaults:
            kwargs['sync_max_delay'] = int(defaults['sync_max_delay'])

        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif 'sync_min_delay' in defaults:
            kwargs['sync_min_delay'] = int(defaults['sync_min_delay'])


        if 'symdevs@'+rcEnv.nodename in conf.options(s):
            symdevs = conf.get(s, 'symdevs@'+rcEnv.nodename).split()
        if 'symdevs' in conf.options(s) and symdevs == []:
            symdevs = conf.get(s, 'symdevs').split()
        if len(symdevs) == 0:
            svc.log.error("config file section %s must have symdevs or symdevs@node set" % s)
            return
        else:
            kwargs['symdevs'] = symdevs

        if conf.has_option(s, 'precopy_timeout'):
            kwargs['precopy_timeout'] = conf.getint(s, 'precopy_timeout')

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)
        r = sc.syncSymclone(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_netapp(svc, conf):
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if conf.has_option(s, 'type') and \
           conf.get(s, 'type') != 'netapp':
            continue

        if not conf.has_option(s, 'type'):
            continue

        if not conf.has_option(s, 'path'):
            svc.log.error("config file section %s must have path set" % s)
            return
        if not conf.has_option(s, 'user'):
            svc.log.error("config file section %s must have user set" % s)
            return

        kwargs = {}
        defaults = conf.defaults()
        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif 'sync_max_delay' in defaults:
            kwargs['sync_max_delay'] = int(defaults['sync_max_delay'])

        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif 'sync_min_delay' in defaults:
            kwargs['sync_min_delay'] = int(defaults['sync_min_delay'])

        filers = {}
        if 'filer' in conf.options(s):
            for n in svc.nodes | svc.drpnodes:
                filers[n] = conf.get(s, 'filer')
        for o in conf.options(s):
            if 'filer@' not in o:
                continue
            (filer, node) = o.split('@')
            filers[node] = conf.get(s, o)
        if rcEnv.nodename not in filers:
            svc.log.error("config file section %s must have filer@%s set" %(s, rcEnv.nodename))

        kwargs['filers'] = filers
        kwargs['path'] = conf.get(s, 'path')
        kwargs['user'] = conf.get(s, 'user')
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)

        r = resSyncNetapp.syncNetapp(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_syncs_rsync(svc, conf):
    """Add mandatory node-to-nodes and node-to-drpnode synchronizations, plus
    the those described in the config file.
    """
    add_mandatory_syncs(svc)

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
        kwargs['src'] = conf.get(s, "src").split()
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

        defaults = conf.defaults()
        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif 'sync_min_delay' in defaults:
            kwargs['sync_min_delay'] = int(defaults['sync_min_delay'])

        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif 'sync_max_delay' in defaults:
            kwargs['sync_max_delay'] = int(defaults['sync_max_delay'])

        targethash = {}
        if 'nodes' in target: targethash['nodes'] = svc.nodes
        if 'drpnodes' in target: targethash['drpnodes'] = svc.drpnodes
        kwargs['target'] = targethash
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s, svc)
        kwargs['optional'] = get_optional(conf, s)

        r = resSyncRsync.Rsync(**kwargs)
        add_triggers(r, conf, s)
        svc += r

def add_apps(svc, conf):
        if svc.svcmode in rcEnv.vt_supported:
            resApp = __import__('resAppVm')
        else:
            resApp = __import__('resApp')

        r = resApp.Apps(runmethod=svc.runmethod)
        svc += r

def setup_logging():
	"""Setup logging to stream + logfile, and logfile rotation
	class Logger instance name: 'log'
	"""
	global log
	log = rcLogger.initLogger('INIT')

def syncnodes(self):
	"""Run all sync jobs to peer nodes for the service
	"""
	for s in self.syncs:
		if s.syncnodes() != 0: return 1

def syncdrp(self):
	"""Run all sync jobs to drp nodes for the service
	"""
	for s in self.syncs:
		if s.syncdrp() != 0: return 1

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
    allowed_clustertype = ['failover', 'allactive', 'flex', 'autoflex']
    if svc.clustertype not in allowed_clustertype:
        svc.log.error("invalid cluster type '%s'. allowed: %s"%(svc.svcname, svc.clustertype, ', '.join(allowed_clustertype)))
        del(svc)
        return None

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
        svc.flex_cpu_high_threshold = 10
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
        svc.autostart_node = defaults["autostart_node"]
    else:
        svc.autostart_node = ''

    if "drp_type" in defaults:
        svc.drp_type = defaults["drp_type"]
    else:
        svc.drp_type = ''

    if "comment" in defaults:
        svc.comment = defaults["comment"]
    else:
        svc.comment = ''

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
    except ex.excInitError:
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
        if onlyprimary and svc.autostart_node != rcEnv.nodename:
            continue
        if onlysecondary and svc.autostart_node == rcEnv.nodename:
            continue
        services[svc.svcname] = svc
        if svc.collector_outdated():
            svc.action('push')
    return [ s for n ,s in sorted(services.items()) ]

def toggle_one(svcname, rids=[], disable=True):
    if len(svcname) == 0:
        print >>sys.stderr, "service name must not be empty"
        return 1
    if svcname not in list_services():
        print >>sys.stderr, "service", svcname, "does not exist"
        return 1
    envfile = os.path.join(rcEnv.pathetc, svcname+'.env')
    conf = ConfigParser.RawConfigParser()
    conf.read(envfile)
    for rid in rids:
        if not conf.has_section(rid):
            print >>sys.stderr, "service", svcname, "has not resource", rid
            continue
        conf.set(rid, "disable", disable)
    try:
       f = open(envfile, 'w')
    except:
        print >>sys.stderr, "failed to open", envfile, "for writing"
        return 1
    conf.write(f)
    return 0

def disable_one(svcname, rids=[]):
    return toggle_one(svcname, rids, disable=True)

def disable(svcnames, rid=[]):
    fix_default_section(svcnames)
    if len(rid) == 0:
        print "no resource flagged for disabling"
        return 0
    r = 0
    for svcname in svcnames:
        r |= disable_one(svcname, rid)
    return r

def enable_one(svcname, rids=[]):
    return toggle_one(svcname, rids, disable=False)

def enable(svcnames, rid=[]):
    fix_default_section(svcnames)
    if len(rid) == 0:
        print "no resource flagged for enabling"
        return 0
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

def create(svcname, resources=[], interactive=False):
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
        keys = KeyDict()
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
    if not os.path.islink(svcname+'.d'):
        os.unlink(svcname+'.d')
    if not os.path.exists(svcname+'.d'):
        os.symlink(initdir, svcname+'.d')

def update(svcname, resources=[], interactive=False):
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
