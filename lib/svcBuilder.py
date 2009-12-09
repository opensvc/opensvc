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

from rcGlobalEnv import *
from freezer import Freezer
from rcNode import discover_node
from rcUtilities import *
import rcOptParser
import rcLogger
import rcAddService
import resRsync
import resApp

check_privs()

pathsvc = os.path.join(os.path.dirname(__file__), '..')
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
    return 1 # raise something instead ?

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
        if svc.svcmode  == 'lxc':
            ip = __import__('resIp'+rcEnv.sysname+'Lxc')
            r = ip.Ip(svc.svcname, ipdev, ipname)
        else:
            ip = __import__('resIp'+rcEnv.sysname)
            r = ip.Ip(ipdev, ipname)
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
        vg = __import__('resVg'+rcEnv.sysname)
        r = vg.Vg(name)
        set_optional_and_disable(r, conf, s)
        set_scsireserv(r, conf, s)
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

def add_mounts(svc, conf):
    """Parse the configuration file and add a fs object for each [fs#n]
    section. Fs objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('fs#[0-9]', s, re.I) is None:
            continue
        dev = conf.get(s, "dev")
        mnt = conf.get(s, "mnt")
        type = conf.get(s, "type")
        mnt_opt = conf.get(s, "mnt_opt")
        mount = __import__('resMount'+rcEnv.sysname)
        r = mount.Mount(mnt, dev, type, mnt_opt)
        set_optional_and_disable(r, conf, s)
        #set_scsireserv(r, conf, s)
        r.svc = svc
        svc += r

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
    dst = os.path.join(rcEnv.pathetc)
    exclude = []
    targethash = {'nodes': svc.nodes, 'drpnodes': svc.drpnodes}
    r = resRsync.Rsync(src, dst, exclude, targethash)
    r.svc = svc
    svc += r

    """2
    """
    targethash = {'drpnodes': svc.drpnodes}
    """Reparent all PRD backed-up file in /DR.opensvc/node on the drpnode
    """
    dst = os.path.join(rcEnv.drp_path, rcEnv.nodename)
    for src, exclude in rcEnv.drp_sync_files:
        """'-R' triggers rsync relative mode
        """
        src = [ s for s in src if os.path.exists(s) ]
        r = resRsync.Rsync(src, dst, ['-R']+exclude, targethash)
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

        targethash = {}
        if 'nodes' in target: targethash['nodes'] = svc.nodes
        if 'drpnodes' in target: targethash['drpnodes'] = svc.drpnodes

        r = resRsync.Rsync(src=src,
                           dst=dst,
                           exclude=exclude,
                           target=targethash,
                           dstfs=dstfs,
                           snap=snap)
        set_optional_and_disable(r, conf, s)
        r.svc = svc
        svc += r

def add_apps(svc, conf):
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
    if os.path.isfile(svcconf):
        conf = ConfigParser.RawConfigParser()
        conf.read(svcconf)
        if conf.has_option("default", "mode"):
            svcmode = conf.get("default", "mode")

    #
    # dynamically import the module matching the service mode
    # and instanciate a service
    #
    log.debug('service mode = ' + svcmode)
    mod , svc_class_name = svcmode_mod_name(svcmode)
    svcMod = __import__(mod)
    svc = getattr(svcMod, svc_class_name)(name)
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

    # prune not managed service
    if rcEnv.nodename not in svc.nodes | svc.drpnodes:
        log.error('service %s not managed here' % name)
        del(svc)
        return None

    if conf.has_option("default", "service_type"):
        svc.svctype = conf.get("default", "service_type")
    else:
        svc.svctype = ''

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

    #
    # instanciate resources
    #
    add_ips(svc, conf)
    add_loops(svc, conf)
    add_vgs(svc, conf)
    add_pools(svc, conf)
    add_mounts(svc, conf)
    add_syncs(svc, conf)
    add_apps(svc, conf)

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

