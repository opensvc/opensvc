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
from freezer import Freezer
from rcNode import discover_node
from rcUtilities import *
import rcOptParser
import rcLogger
import rcAddService
import resRsync
import rcExceptions as ex

check_privs()

pathsvc = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
pathetc = os.path.join(pathsvc, 'etc')

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
    raise

def set_optional(resource, conf, section):
    if conf.has_option(section, 'optional') and \
       conf.getboolean(section, "optional") == True:
            resource.set_optional()

def set_disable(resource, conf, section):
    if conf.has_option(section, 'disable') and \
       conf.getboolean(section, "disable") == True:
            resource.disable()

def set_optional_and_disable(resource, conf, section):
    set_optional(resource, conf, section)
    set_disable(resource, conf, section)

def set_scsireserv(resource, conf, section):
    """scsireserv = true can be set globally or in a specific
    resource section
    """
    if conf.has_option('default', 'scsireserv') and \
       conf.getboolean('default', 'scsireserv') == True:
           resource.set_scsireserv()
    if conf.has_option(section, 'scsireserv') and \
       conf.getboolean(section, 'scsireserv') == True:
           resource.set_scsireserv()

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
    return always_on

def add_ips(svc, conf):
    """Parse the configuration file and add an ip object for each [ip#n]
    section. Ip objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('ip#[0-9]', s, re.I) is None:
            continue
        if conf.has_option(s, "ipname@"+rcEnv.nodename):
            ipname = conf.get(s, "ipname@"+rcEnv.nodename)
        elif conf.has_option(s, "ipname"):
            ipname = conf.get(s, "ipname")
        else:
            svc.log.error("nor ipname and ipname@%s defined in config file section %s"%(rcEnv.nodename, s))
            ipname = None
            continue
        if conf.has_option(s, "ipdev@"+rcEnv.nodename):
            ipdev = conf.get(s, "ipdev@"+rcEnv.nodename)
        elif conf.has_option(s, "ipdev"):
            ipdev = conf.get(s, "ipdev")
        else:
            svc.log.debug('add_ips ipdev not found in ip section' + s)
            ipdev = None
            continue
        if conf.has_option(s, "netmask"):
            netmask = conf.get(s, "netmask")
        else:
            netmask = None
        if svc.svcmode  == 'lxc':
            ip = __import__('resIp'+rcEnv.sysname+'Lxc')
            r = ip.Ip(svc.svcname, ipdev, ipname)
        elif svc.svcmode  == 'hpvm':
            ip = __import__('resIp'+'HpVm')
            r = ip.Ip(svc.vmname, ipdev, ipname)
        else:
            ip = __import__('resIp'+rcEnv.sysname)
            r = ip.Ip(ipdev, ipname, netmask)
        set_optional_and_disable(r, conf, s)
        r.svc = svc
        svc += r

def add_loops(svc, conf):
    """Parse the configuration file and add a loop object for each [loop#n]
    section. Loop objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('loop#[0-9]', s, re.I) is None:
            continue
        file = conf.get(s, "file")
        loop = __import__('resLoop'+rcEnv.sysname)
        r = loop.Loop(file)
        set_optional_and_disable(r, conf, s)
        r.svc = svc
        svc += r

def add_vgs(svc, conf):
    """Parse the configuration file and add a vg object for each [vg#n]
    section. Vg objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('vg#[0-9]', s, re.I) is None:
            continue
        name = conf.get(s, "vgname")
        if conf.has_option(s, "dsf"):
            dsf = conf.getboolean(s, "dsf")
        else:
            dsf = True
        always_on = always_on_nodes_set(svc, conf, s)
        vg = __import__('resVg'+rcEnv.sysname)
        r = vg.Vg(name, always_on=always_on)
        set_optional_and_disable(r, conf, s)
        set_scsireserv(r, conf, s)
        r.svc = svc
        r.dsf = dsf
        svc += r

def add_vmdg(svc, conf):
    if not conf.has_section('vmdg'):
        return
    if svc.svcmode == 'hpvm':
        vg = __import__('resVgHpVm')
    else:
        return
    r = vg.Vg('vmdg')
    set_optional_and_disable(r, conf, 'vmdg')
    set_scsireserv(r, conf, 'vmdg')
    r.svc = svc
    svc += r

def add_pools(svc, conf):
    """Parse the configuration file and add a pool object for each [pool#n]
    section. Pools objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('pool#[0-9]', s, re.I) is None:
            continue
        name = conf.get(s, "poolname")
        pool = __import__('resZfs')
        r = pool.Pool(name)
        set_optional_and_disable(r, conf, s)
        set_scsireserv(r, conf, s)
        r.svc = svc
        svc += r

def add_filesystems(svc, conf):
    """Parse the configuration file and add a fs object for each [fs#n]
    section. Fs objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('fs#[0-9]', s, re.I) is None:
            continue
        dev = conf.get(s, "dev")
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
        always_on = always_on_nodes_set(svc, conf, s)
        mount = __import__('resMount'+rcEnv.sysname)
        r = mount.Mount(mnt, dev, type, mnt_opt, always_on)
        set_optional_and_disable(r, conf, s)
        #set_scsireserv(r, conf, s)
        r.svc = svc
        svc += r

def add_mandatory_syncs(svc):
    def list_mapfiles():
        pattern = os.path.join(rcEnv.pathvar, 'vg_'+svc.svcname+'_*.map')
        files = glob.glob(pattern)
        if len(files) > 0:
            return files
        return []

    def list_mksffiles():
        pattern = os.path.join(rcEnv.pathvar, 'vg_'+svc.svcname+'_*.mksf')
        files = glob.glob(pattern)
        if len(files) > 0:
            return files
        return []

    def list_hpvmconffiles():
        a = []
        if svc.svcmode != 'hpvm':
            return a
        guest = os.path.join(os.sep, 'var', 'opt', 'hpvm', 'guests', svc.vmname)
        uuid = os.path.realpath(guest)
        if os.path.exists(guest):
            a.append(guest)
        if os.path.exists(uuid):
            a.append(uuid)
        return a

    """Mandatory files to sync:
    1/ to all nodes: service definition
    2/ to drpnodes: system files to replace on the drpnode in case of startdrp

    Set sync_min_delay to 24h for system's files sync to DR nodes
    """

    """1
    """
    src = []
    src.append(os.path.join(rcEnv.pathetc, svc.svcname))
    src.append(os.path.join(rcEnv.pathetc, svc.svcname+'.env'))
    src.append(os.path.join(rcEnv.pathetc, svc.svcname+'.d'))
    src += list_mapfiles()
    src += list_mksffiles()
    src += list_hpvmconffiles()
    dst = os.path.join("/")
    exclude = []
    targethash = {'nodes': svc.nodes, 'drpnodes': svc.drpnodes}
    r = resRsync.Rsync(src, dst, ['-R']+exclude, targethash, internal=True)
    r.svc = svc
    svc += r

    """2
    """
    targethash = {'drpnodes': svc.drpnodes}
    """ Reparent all PRD backed-up file in drp_path/node on the drpnode
    """
    dst = os.path.join(rcEnv.drp_path, rcEnv.nodename)
    for src, exclude in rcEnv.drp_sync_files:
        """'-R' triggers rsync relative mode
        """
        src = [ s for s in src if os.path.exists(s) ]
        if len(src) == 0:
            continue
        r = resRsync.Rsync(src, dst, ['-R']+exclude, targethash, internal=True, sync_min_delay=1430)
        r.svc = svc
        svc += r

def add_syncs(svc, conf):
    """Add mandatory node-to-nodes and node-to-drpnode synchronizations, plus
    the those described in the config file.
    """
    add_mandatory_syncs(svc)

    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue

        if not conf.has_option(s, 'src') or \
           not conf.has_option(s, 'dst'):
            log.error("config file section %s must have src and dst set" % s)
            return 1
        src = conf.get(s, "src").split()
        dst = conf.get(s, "dst")

        if conf.has_option(s, 'dstfs'):
            dstfs = conf.get(s, 'dstfs')
        else:
            dstfs = None

        if conf.has_option(s, 'exclude'):
            exclude = conf.get(s, 'exclude').split()
        else:
            exclude = []

        if conf.has_option(s, 'snap'):
            snap = conf.getboolean(s, 'snap')
        else:
            snap = False

        if conf.has_option(s, 'target'):
            target = conf.get(s, 'target').split()
        else:
            target = ['nodes', 'drpnodes']

        if conf.has_option(s, 'bwlimit'):
            bwlimit = conf.get(s, 'bwlimit')
        else:
            bwlimit = None

        if conf.has_option(s, 'sync_min_delay'):
            sync_min_delay = conf.get(s, 'sync_min_delay')
        elif conf.has_option('default', 'sync_min_delay'):
            sync_min_delay = conf.get('default', 'sync_min_delay')
        else:
            sync_min_delay = 30

        targethash = {}
        if 'nodes' in target: targethash['nodes'] = svc.nodes
        if 'drpnodes' in target: targethash['drpnodes'] = svc.drpnodes

        r = resRsync.Rsync(src=src,
                           dst=dst,
                           exclude=exclude,
                           target=targethash,
                           dstfs=dstfs,
                           bwlimit=bwlimit,
                           sync_min_delay=sync_min_delay,
                           snap=snap)
        set_optional_and_disable(r, conf, s)
        r.svc = svc
        svc += r

def add_apps(svc, conf):
        if svc.svcmode in ['hpvm']:
            resApp = __import__('resAppVm')
            r = resApp.Apps(hostname=svc.vmname)
        else:
            resApp = __import__('resApp')
            r = resApp.Apps()
        r.svc = svc
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

    svcconf = os.path.join(rcEnv.pathetc, name) + '.env'
    svcinitd = os.path.join(rcEnv.pathetc, name) + '.d'
    logfile = os.path.join(rcEnv.pathlog, name) + '.log'
    rcEnv.logfile = logfile

    setup_logging()
    if name == "rcService":
            log.error("do not execute rcService directly")
            return None

    #
    # print stuff we determined so far
    #
    log.debug('sysname = ' + rcEnv.sysname)
    log.debug('nodename = ' + rcEnv.nodename)
    log.debug('machine = ' + rcEnv.machine)
    log.debug('pathsvc = ' + rcEnv.pathsvc)
    log.debug('pathbin = ' + rcEnv.pathbin)
    log.debug('pathetc = ' + rcEnv.pathetc)
    log.debug('pathlib = ' + rcEnv.pathlib)
    log.debug('pathlog = ' + rcEnv.pathlog)
    log.debug('pathtmp = ' + rcEnv.pathtmp)
    log.debug('service name = ' + name)
    log.debug('service config file = ' + svcconf)
    log.debug('service log file = ' + logfile)

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
            vmname = conf.get("default", "vm_name")
            kwargs['vmname'] = vmname

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
        add_syncs(svc, conf)
        add_apps(svc, conf)
    except ex.excInitError:
        return None

    return svc

def is_service(f):
    rcService = os.path.join(pathsvc, 'bin', 'rcService')
    if os.path.realpath(f) != os.path.realpath(rcService):
        return False
    if not os.path.exists(f + '.env'):
        return False
    return True

def build_services(status=None, svcnames=[], onlyprimary=False):
    """returns a list of all services of status matching the specified status.
    If no status is specified, returns all services
    """
    services = {}
    for name in os.listdir(pathetc):
        if len(svcnames) > 0 and name not in svcnames:
            continue
        if not is_service(os.path.join(pathetc, name)):
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

