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
import rcLogger
import logging

def get_supported_actions(conf):
	__actions = []
	if conf.has_section("fs1") is True or conf.has_section("disk1"):
		__actions.extend(["mount", "umount"])
	if conf.has_section("nfs1") is True:
		__actions.extend(["mountnfs", "umountnfs"])
	if conf.has_section("ip1") is True:
		__actions.extend(["startip", "stopip"])
	if conf.get("default", "mode") is "lxc":
		__actions.append("configure")
	return __actions

class svc:
	"""This base class exposes actions available to all type of services,
	like stop, start, ...
	It's meant to be enriched by inheriting class for specialized services,
	like LXC containers, ...
	"""

	#
	# actions advertized to user by option parser (seed)
	#
	actions = [ 'syncnodes', 'syncdrp', 'start', 'stop', 'startapp', 'stopapp' ]

	def __init__(self, name):
		discover_node()
		#
		# setup logging to stream + logfile, and logfile rotation
		# class Logger instance name: 'log'
		#
		log = rcLogger.logger('INIT')

		if name == "rcService":
			print "do not execute rcService directly"
			sys.exit(1)

		#
		# parse service configuration file
		# class RawConfigParser instance name: 'conf'
		#
		if os.path.isfile(rcEnv.svcconf):
			self.cf = ConfigParser.RawConfigParser()
			self.cf.read(rcEnv.svcconf)
			self.actions.extend(get_supported_actions(self.cf))
		else:
			self.actions = [ "create" ]

		#
		# parse command line
		# class svcOptionParser instance name: 'parser'
		#
		self.parser = rcOptParser.svcOptionParser(self.actions)
		if self.parser.options.debug is True:
			log.setLevel(logging.DEBUG)
		else:
			log.setLevel(logging.INFO)

		log.debug('service name = ' + rcEnv.svcname)
		log.debug('service config file = ' + rcEnv.svcconf)
                log.debug('service log file = ' + rcEnv.logfile)
		log.debug('service supported actions = ' + str(self.actions))
                log.debug('sysname = ' + rcEnv.sysname)
                log.debug('nodename = ' + rcEnv.nodename)
                log.debug('machine = ' + rcEnv.machine)
                log.debug('capabilities = ' + str(rcEnv.capabilities))
                log.debug('pathsvc = ' + rcEnv.pathsvc)
                log.debug('pathbin = ' + rcEnv.pathbin)
                log.debug('pathetc = ' + rcEnv.pathetc)
                log.debug('pathlib = ' + rcEnv.pathlib)
                log.debug('pathlog = ' + rcEnv.pathlog)
                log.debug('pathtmp = ' + rcEnv.pathtmp)

		#
		# instanciate appropiate actions class
		# class do instance name: 'do'
		#
		self.do = rcAction.do()
		getattr(self.do, self.parser.action)()

