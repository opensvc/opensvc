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
import glob
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
os.environ['PATH'] = '/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'

def svcmode_mod_name(svcmode=''):
    """Returns (moduleName, serviceClassName) implementing the class for
    a given service mode. For example:
    lxc    => ('svcLxc', 'SvcLxc')
    zone   => ('svcZone', 'SvcZone')
    hosted => ('svcHosted', 'SvcHosted')
    """
    if svcmode == 'lxc':
        return ('svcLxc', 'SvcLxc')
    elif svcmode == 'zone':
        return ('svcZone', 'SvcZone')
    elif svcmode == 'hosted':
        return ('svcHosted', 'SvcHosted')
    elif svcmode == 'hpvm':
        return ('svcHpVm', 'SvcHpVm')
    elif svcmode == 'ldom':
        return ('svcLdom', 'SvcLdom')
    elif svcmode == 'kvm':
        return ('svcKvm', 'SvcKvm')
    raise

def get_tags(conf, section):
    if conf.has_option(section, 'tags'):
       return set(conf.get(section, "tags").split())
    return set([])

def get_optional(conf, section):
    if conf.has_option(section, 'optional'):
       return conf.getboolean(section, "optional")
    return False

def get_disabled(conf, section):
    if conf.has_option(section, 'disable'):
       return conf.getboolean(section, "disable")
    return False

def need_scsireserv(resource, conf, section):
    """scsireserv = true can be set globally or in a specific
    resource section
    """
    if conf.has_option(section, 'scsireserv'):
       if conf.getboolean(section, 'scsireserv') == True:
           return True
       else:
           return False
    elif conf.has_option('default', 'scsireserv') and \
       conf.getboolean('default', 'scsireserv') == True:
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
        elif svc.svcmode  == 'kvm':
            ip = __import__('resIp'+'Kvm')
        elif svc.svcmode  == 'hpvm':
            ip = __import__('resIp'+'HpVm')
        elif svc.svcmode  == 'ldom':
            ip = __import__('resIp'+'Ldom')
        elif svc.svcmode  == 'zone':
            ip = __import__('resIp'+'Zone')
        else:
            ip = __import__('resIp'+rcEnv.sysname)
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
        kwargs['disabled'] = get_disabled(conf, s)
        kwargs['optional'] = get_optional(conf, s)
        r = ip.Ip(**kwargs)
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
        kwargs['disabled'] = get_disabled(conf, s)
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
        kwargs['disabled'] = get_disabled(conf, s)
        kwargs['optional'] = get_optional(conf, s)

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
    kwargs['disabled'] = get_disabled(conf, 'vmdg')
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
        kwargs['disabled'] = get_disabled(conf, s)
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
                globalfs = conf.get(s, "globalfs")
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
        kwargs['disabled'] = get_disabled(conf, s)
        kwargs['optional'] = get_optional(conf, s)

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
    if os.path.exists(localrc):
        src.append(localrc)
    for rs in svc.resSets:
        for r in rs.resources:
            src += r.files_to_sync()
    dst = os.path.join("/")
    exclude = ['--exclude=*.core']
    targethash = {'nodes': svc.nodes, 'drpnodes': svc.drpnodes}
    r = resSyncRsync.Rsync(rid="sync#i0", src=src, dst=dst,
                           exclude=['-R']+exclude, target=targethash,
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
                           exclude=['-R']+exclude, target=targethash,
                           internal=True)
        svc += r

def add_syncs(svc, conf):
    add_syncs_rsync(svc, conf)
    add_syncs_netapp(svc, conf)
    add_syncs_symclone(svc, conf)
    add_syncs_dds(svc, conf)

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
            log.error("config file section %s must have src set" % s)
            return
        else:
            kwargs['src'] = conf.get(s, 'src')

        if not conf.has_option(s, 'dst'):
            kwargs['dst'] = conf.get(s, 'src')
        else:
            kwargs['dst'] = conf.get(s, 'dst')

        if not conf.has_option(s, 'target'):
            log.error("config file section %s must have target set" % s)
            return
        else:
            kwargs['target'] = conf.get(s, 'target').split()

        if conf.has_option(s, 'sender'):
            kwargs['sender'] = conf.get(s, 'sender')

        if conf.has_option(s, 'snap_size'):
            kwargs['snap_size'] = conf.getint(s, 'snap_size')

        if conf.has_option(s, 'delta_store'):
            kwargs['delta_store'] = conf.get(s, 'delta_store')

        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif conf.has_option('default', 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint('default', 'sync_max_delay')

        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif conf.has_option('default', 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint('default', 'sync_min_delay')

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s)
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
            log.error("config file section %s must have symdg set" % s)
            return
        else:
            kwargs['symdg'] = conf.get(s, 'symdg')

        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif conf.has_option('default', 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint('default', 'sync_max_delay')

        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif conf.has_option('default', 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint('default', 'sync_min_delay')


        if 'symdevs@'+rcEnv.nodename in conf.options(s):
            symdevs = conf.get(s, 'symdevs@'+rcEnv.nodename).split()
        if 'symdevs' in conf.options(s) and symdevs == []:
            symdevs = conf.get(s, 'symdevs').split()
        if len(symdevs) == 0:
            log.error("config file section %s must have symdevs or symdevs@node set" % s)
            return
        else:
            kwargs['symdevs'] = symdevs

        if conf.has_option(s, 'precopy_timeout'):
            kwargs['precopy_timeout'] = conf.getint(s, 'precopy_timeout')

        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s)
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
            log.error("config file section %s must have path set" % s)
            return
        if not conf.has_option(s, 'user'):
            log.error("config file section %s must have user set" % s)
            return

        kwargs = {}
        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif conf.has_option('default', 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint('default', 'sync_max_delay')

        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif conf.has_option('default', 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint('default', 'sync_min_delay')

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
            log.error("config file section %s must have filer@%s set" %(s, rcEnv.nodename))

        kwargs['filers'] = filers
        kwargs['path'] = conf.get(s, 'path')
        kwargs['user'] = conf.get(s, 'user')
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s)
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
            log.error("config file section %s must have src and dst set" % s)
            return

        kwargs = {}
        kwargs['src'] = conf.get(s, "src").split()
        kwargs['dst'] = conf.get(s, "dst")

        if conf.has_option(s, 'dstfs'):
            kwargs['dstfs'] = conf.get(s, 'dstfs')

        if conf.has_option(s, 'exclude'):
            kwargs['exclude'] = conf.get(s, 'exclude').split()

        if conf.has_option(s, 'snap'):
            kwargs['snap'] = conf.getboolean(s, 'snap')

        if conf.has_option(s, 'target'):
            target = conf.get(s, 'target').split()
        else:
            target = []

        if conf.has_option(s, 'bwlimit'):
            kwargs['bwlimit'] = conf.get(s, 'bwlimit')

        if conf.has_option(s, 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint(s, 'sync_min_delay')
        elif conf.has_option('default', 'sync_min_delay'):
            kwargs['sync_min_delay'] = conf.getint('default', 'sync_min_delay')

        if conf.has_option(s, 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint(s, 'sync_max_delay')
        elif conf.has_option('default', 'sync_max_delay'):
            kwargs['sync_max_delay'] = conf.getint('default', 'sync_max_delay')

        targethash = {}
        if 'nodes' in target: targethash['nodes'] = svc.nodes
        if 'drpnodes' in target: targethash['drpnodes'] = svc.drpnodes
        kwargs['target'] = targethash
        kwargs['rid'] = s
        kwargs['tags'] = get_tags(conf, s)
        kwargs['disabled'] = get_disabled(conf, s)
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
	logging.setLoggerClass(rcLogger.Logger)
	global log
	log = logging.getLogger('INIT')
	if '--debug' in sys.argv:
		rcEnv.loglevel = logging.DEBUG
		log.setLevel(logging.DEBUG)
	elif '--warn' in sys.argv:
		rcEnv.loglevel = logging.WARNING
		log.setLevel(logging.WARNING)
	elif '--error' in sys.argv:
		rcEnv.loglevel = logging.ERROR
		log.setLevel(logging.ERROR)
	else:
		rcEnv.loglevel = logging.INFO
		log.setLevel(logging.INFO)

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
        if conf.has_option("default", "mode"):
            svcmode = conf.get("default", "mode")
        if conf.has_option("default", "vm_name"):
            if svcmode not in rcEnv.vt_supported:
                log.error("can not set 'vm_name' with '%s' mode in %s env"%(svcmode, name))
                return None
            vmname = conf.get("default", "vm_name")
            kwargs['vmname'] = vmname
        if conf.has_option("default", "guest_os"):
            if svcmode not in rcEnv.vt_supported:
                log.error("can not set 'guest_os' with '%s' mode in %s env"%(svcmode, name))
                return None
            guestos = conf.get("default", "guest_os")
            kwargs['guestos'] = guestos
        elif svcmode in rcEnv.vt_supported:
            guestos = rcEnv.sysname
            kwargs['guestos'] = guestos

    #
    # dynamically import the module matching the service mode
    # and instanciate a service
    #
    log.debug('service mode = ' + svcmode)
    mod , svc_class_name = svcmode_mod_name(svcmode)
    svcMod = __import__(mod)
    svc = getattr(svcMod, svc_class_name)(**kwargs)
    svc.svcmode = svcmode

    #
    # Store useful properties
    #
    svc.logfile = logfile
    svc.conf = svcconf
    svc.initd = svcinitd

    #
    # Setup service properties from config file content
    #

    if conf.has_option("default", "nodes"):
        svc.nodes = set(conf.get("default", "nodes").split())
        svc.nodes -= set([''])
    else:
        svc.nodes = set([])

    if conf.has_option("default", "drpnodes"):
        svc.drpnodes = set(conf.get("default", "drpnodes").split())
        svc.drpnodes -= set([''])
    else:
        svc.drpnodes = set([])

    if conf.has_option("default", "drpnode"):
        svc.drpnode = conf.get("default", "drpnode")
        svc.drpnodes |= set([svc.drpnode])
        svc.drpnodes -= set([''])
    else:
        svc.drpnode = ''

    """ prune not managed service
    """
    if rcEnv.nodename not in svc.nodes | svc.drpnodes:
        log.error('service %s not managed here' % name)
        del(svc)
        return None

    if conf.has_option("default", "service_type"):
        svc.svctype = conf.get("default", "service_type")
    else:
        svc.svctype = ''

    allowed_svctype = ['PRD', 'DEV', 'TMP', 'TST']
    if svc.svctype not in allowed_svctype:
        log.error('service %s type %s is not a known service type (%s)'%(svc.svcname, svc.svctype, ', '.join(allowed_svctype)))
        del(svc)
        return None

    """ prune service whose service type does not match host mode
    """
    if svc.svctype != 'PRD' and rcEnv.host_mode == 'PRD':
        log.error('service %s type %s is not allowed to run on this node (host mode %s)' % (svc.svcname, svc.svctype, rcEnv.host_mode))
        del(svc)
        return None

    if conf.has_option("default", "autostart_node"):
        svc.autostart_node = conf.get("default", "autostart_node")
    else:
        svc.autostart_node = ''

    if conf.has_option("default", "drp_type"):
        svc.drp_type = conf.get("default", "drp_type")
    else:
        svc.drp_type = ''

    if conf.has_option("default", "comment"):
        svc.comment = conf.get("default", "comment")
    else:
        svc.comment = ''

    if conf.has_option("default", "app"):
        svc.app = conf.get("default", "app")
    else:
        svc.app = ''

    if conf.has_option("default", "drnoaction"):
        svc.drnoaction = conf.get("default", "drnoaction")
    else:
        svc.drnoaction = False

    if conf.has_option("default", "bwlimit"):
        svc.bwlimit = conf.get("default", "bwlimit")
    else:
        svc.bwlimit = None

    #
    # instanciate resources
    #
    try:
        add_ips(svc, conf)
        add_loops(svc, conf)
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

def build_services(status=None, svcnames=[], onlyprimary=False):
    """returns a list of all services of status matching the specified status.
    If no status is specified, returns all services
    """
    services = {}
    for name in os.listdir(rcEnv.pathetc):
        if len(svcnames) > 0 and name not in svcnames:
            continue
        if not is_service(os.path.join(rcEnv.pathetc, name)):
            continue
        svc = build(name)
        if svc is None :
            continue
        if status is not None and svc.status() != status:
            continue
        if onlyprimary and svc.autostart_node != rcEnv.nodename:
            continue
        services[svc.svcname] = svc
    return [ s for n ,s in sorted(services.items()) ]

