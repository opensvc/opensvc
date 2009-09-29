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
from rcGlobalEnv import *
from rcNode import discover_node
import rcOptParser
import rcAction
import ConfigParser
import logging
import rcLogger
import rcAddService
import rcLXC
import rcHosted

def install_actions(conf):
	"""Setup the class svc methods as per node capabilities and
	service configuration.
	"""
	rcEnv.do = rcAction.do()
	if conf is None:
		rcEnv.actions = [ "create" ]
		rcEnv.do.create = rcAddService.addservice
		return

	generic_actions = [ 'syncnodes', 'syncdrp', 'start', 'stop',
			    'startapp', 'stopapp' ]
	rcEnv.actions = generic_actions
	if conf.has_option("default", "mode"):
		rcEnv.svcmode = conf.get("default", "mode")
	else:
		rcEnv.svcmode = "hosted"

	if rcEnv.svcmode == "hosted":
		rcEnv.do.syncnodes = rcHosted.syncnodes
		rcEnv.do.syncdrp = rcHosted.syncdrp
		rcEnv.do.start = rcHosted.start
		rcEnv.do.stop = rcHosted.stop
		rcEnv.do.startapp = rcHosted.startapp
		rcEnv.do.stopapp = rcHosted.stopapp
		rcEnv.do.start = rcHosted.start
		if conf.has_section("fs1") is True or conf.has_section("disk1"):
			rcEnv.actions.extend(["mount", "umount"])
			rcEnv.do.mount = rcHosted.mount
			rcEnv.do.umount = rcHosted.umount
		if conf.has_section("nfs1") is True:
			rcEnv.actions.extend(["mountnfs", "umountnfs"])
			rcEnv.do.mountnfs = rcHosted.mountnfs
			rcEnv.do.umountnfs = rcHosted.umountnfs
		if conf.has_section("ip1") is True:
			ipname = conf.get("ip1", "ipname")
			ipdev = conf.get("ip1", "ipdev")
			ip = rcHosted.ip(ipname, ipdev)
			if ip is None:
				log.error("initialization failed for ip1 (%s@%s)" % (ipname, ipdev))
				return 1
			log.debug("initialization succeeded for ip1 (%s@%s)" % (ipname, ipdev))
			rcEnv.actions.extend(["startip", "stopip"])
			rcEnv.do.startip = rcHosted.startip
			rcEnv.do.stopip = rcHosted.stopip
			rcEnv.ips = []
			rcEnv.ips.append(ip)
	elif rcEnv.svcmode == "lxc":
		rcEnv.actions.append("configure")
		rcEnv.do.configure = rcLXC.configure
		rcEnv.do.syncnodes = rcLXC.syncnodes
		rcEnv.do.syncdrp = rcLXC.syncdrp
		rcEnv.do.start = rcLXC.start
		rcEnv.do.stop = rcLXC.stop
		rcEnv.do.startapp = rcLXC.startapp
		rcEnv.do.stopapp = rcLXC.stopapp
		if conf.has_section("fs1") is True or conf.has_section("disk1"):
			rcEnv.actions.extend(["mount", "umount"])
			rcEnv.do.mount = rcLXC.mount
			rcEnv.do.umount = rcLXC.umount
		if conf.has_section("nfs1") is True:
			rcEnv.actions.extend(["mountnfs", "umountnfs"])
			rcEnv.do.mountnfs = rcLXC.mountnfs
			rcEnv.do.umountnfs = rcLXC.umountnfs
		if conf.has_section("ip1") is True:
			rcEnv.actions.extend(["startip", "stopip"])
			rcEnv.do.startip = rcLXC.startip
			rcEnv.do.stopip = rcLXC.stopip
	return 0

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


class svc():
	"""This base class exposes actions available to all type of services,
	like stop, start, ...
	It's meant to be enriched by inheriting class for specialized services,
	like LXC containers, ...
	"""

	def __init__(self, name):
		setup_logging()
		if name == "rcService":
			log.error("do not execute rcService directly")
			return None

		#
		# print stuff we determined so far
		#
                log.debug('pathsvc = ' + rcEnv.pathsvc)
                log.debug('pathbin = ' + rcEnv.pathbin)
                log.debug('pathetc = ' + rcEnv.pathetc)
                log.debug('pathlib = ' + rcEnv.pathlib)
                log.debug('pathlog = ' + rcEnv.pathlog)
                log.debug('pathtmp = ' + rcEnv.pathtmp)
		log.debug('service name = ' + rcEnv.svcname)
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
		if os.path.isfile(rcEnv.svcconf):
			self.cf = ConfigParser.RawConfigParser()
			self.cf.read(rcEnv.svcconf)
			if install_actions(self.cf) != 0: return None
			log.debug('service mode = ' + rcEnv.svcmode)
		else:
			install_actions(None)
		log.debug('service supported actions = ' + str(rcEnv.actions))

		#
		# parse command line
		# class svcOptionParser instance name: 'parser'
		#
		self.parser = rcOptParser.svcOptionParser()

		#
		# instanciate appropiate actions class
		# class do instance name: 'do'
		#
		getattr(rcEnv.do, self.parser.action)()

