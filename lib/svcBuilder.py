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
import rsync

check_privs()

pathsvc = os.path.join(os.path.dirname(__file__), '..')
pathetc = os.path.join(pathsvc, 'etc')

def svcmode_mod_name(svcmode=''):
    """Returns the name of the module implementing the class for
    a given service mode. For example:
    lxc    => svcLxc
    zone   => svcZone
    hosted => svcHosted
    """
    if svcmode == 'lxc':
        return 'svcLxc'
    elif svcmode == 'zone':
        return 'svcZone'
    elif svcmode == 'hosted':
        return 'svcHosted'
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
            ipname = None
            raise
        if conf.has_option(s, "ipdev@"+rcEnv.nodename):
            ipdev = conf.get(s, "ipdev@"+rcEnv.nodename)
        elif conf.has_option(s, "ipdev"):
            ipdev = conf.get(s, "ipdev")
        else:
            ipdev = None
            raise
        ip = __import__('ip'+rcEnv.sysname)
        r = ip.Ip(ipdev, ipname)
        set_optional_and_disable(r, conf, s)
        svc += r

def add_loops(svc, conf):
    """Parse the configuration file and add a loop object for each [loop#n]
    section. Loop objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('loop#[0-9]', s, re.I) is None:
            continue
        file = conf.get(s, "file")
        loop = __import__('loop'+rcEnv.sysname)
        r = loop.Loop(file)
        set_optional_and_disable(r, conf, s)
        svc += r

def add_vgs(svc, conf):
    """Parse the configuration file and add a vg object for each [vg#n]
    section. Vg objects are stored in a list in the service object.
    """
    for s in conf.sections():
        if re.match('vg#[0-9]', s, re.I) is None:
            continue
        name = conf.get(s, "vgname")
        vg = __import__('vg'+rcEnv.sysname)
        r = vg.Vg(name)
        set_optional_and_disable(r, conf, s)
        set_scsireserv(r, conf, s)
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
        mount = __import__('mount'+rcEnv.sysname)
        r = mount.Mount(mnt, dev, type, mnt_opt)
        set_optional_and_disable(r, conf, s)
        #set_scsireserv(r, conf, s)
        svc += r

def add_syncs(svc, conf):
    """Add mandatory node-to-nodes and node-to-drpnode synchronizations, plus
    the those described in the config file.
    """
    for s in conf.sections():
        if re.match('sync#[0-9]', s, re.I) is None:
            continue
        if not conf.has_option(s, 'src') or \
           not conf.has_option(s, 'dst'):
            log.error("config file section %s must have src and dst set" % s)
            return 1
        src = conf.get(s, "src")
        dst = conf.get(s, "dst")
        if conf.has_option(s, 'exclude'):
            exclude = conf.get(s, 'exclude')
        else:
            exclude = ''
        if conf.has_option(s, 'target'):
            target = conf.get(s, 'target').split()
        else:
            target = ['nodes', 'drpnode']

        targethash = {}
        for t in target:
            if conf.has_option("default", t):
                targethash[t] = conf.get("default", t)

        r = rsync.Rsync(src, dst, exclude, targethash)
        set_optional_and_disable(r, conf, s)
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
    rcEnv.logfile = os.path.join(rcEnv.pathlog, name) + '.log'
    rcEnv.svcconf = os.path.join(rcEnv.pathetc, name) + '.env'
    rcEnv.svcinitd = os.path.join(rcEnv.pathetc, name) + '.d'
    rcEnv.sysname, rcEnv.nodename, x, x, rcEnv.machine = os.uname()

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
    log.debug('service config file = ' + rcEnv.svcconf)
    log.debug('service log file = ' + rcEnv.logfile)
    log.debug('service init dir = ' + rcEnv.svcinitd)

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
    if os.path.isfile(rcEnv.svcconf):
            conf = ConfigParser.RawConfigParser()
            conf.read(rcEnv.svcconf)
            if conf.has_option("default", "mode"):
                    svcmode = conf.get("default", "mode")

    #
    # dynamically import the module matching the service mode
    # and instanciate a service
    #
    log.debug('service mode = ' + svcmode)
    mod = svcmode_mod_name(svcmode)
    svcMod = __import__(mod)
    svc = getattr(svcMod, mod)(name)

    #
    # Setup service properties from config file content
    #
    svc.nodes = []
    svc.drpnode = []
    if conf.has_option("default", "nodes"):
        svc.nodes = conf.get("default", "nodes").split()
    if conf.has_option("default", "drpnode"):
        svc.drpnode = conf.get("default", "drpnode").split()
    if conf.has_option("default", "service_type"):
        svc.service_type = conf.get("default", "service_type")
    if conf.has_option("default", "autostart_node"):
        svc.autostart_node = conf.get("default", "autostart_node")
    if conf.has_option("default", "drp_type"):
        svc.drp_type = conf.get("default", "drp_type")
    if conf.has_option("default", "comment"):
        svc.comment = conf.get("default", "comment")
    if conf.has_option("default", "app"):
        svc.app = conf.get("default", "app")
    if conf.has_option("default", "drnoaction"):
        svc.drnoaction = conf.get("default", "drnoaction")

    #
    # instanciate resources
    #
    add_ips(svc, conf)
    add_loops(svc, conf)
    add_vgs(svc, conf)
    add_mounts(svc, conf)
    add_syncs(svc, conf)

    return svc

def is_service(f):
    rcService = os.path.join(pathsvc, 'bin', 'rcService')
    if os.path.realpath(f) != os.path.realpath(rcService):
        return False
    if not os.path.exists(f + '.env'):
        return False
    return True

def build_services(status=None):
    """returns a list of all services of status matching the specified status.
    If no status is specified, returns all services
    """
    services = []
    for name in os.listdir(pathetc):
        if is_service(os.path.join(pathetc, name)):
            svc = build(name)
            if status is None or svc.status() == status:
                services.append(build(name))
    return services

