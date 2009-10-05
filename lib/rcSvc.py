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

from rcGlobalEnv import *
from rcFreeze import Freezer
from rcNode import discover_node
import rcOptParser
import rcLogger
import rcAddService

def svcmode_mod_name(svcmode=''):
	if svcmode == 'lxc':
		return 'rcSvcLxc'
	elif svcmode == 'hosted':
		return 'rcSvcHosted'
	return 1 # raise something instead ?

def add_ips(self):
	for s in self.conf.sections():
		if 'ip' in s:
			ipname = self.conf.get(s, "ipname")
			ipdev = self.conf.get(s, "ipdev")
			self.add_ip(ipname, ipdev)

def add_filesystems(self):
	for s in self.conf.sections():
		if 'fs' in s:
			dev = self.conf.get(s, "dev")
			mnt = self.conf.get(s, "mnt")
			type = self.conf.get(s, "type")
			mnt_opt = self.conf.get(s, "mnt_opt")
			self.add_filesystem(dev, mnt, type, mnt_opt)

def install_actions(self):
	"""Setup the class svc methods as per node capabilities and
	service configuration.
	"""
	if self.conf is None:
		self.create = self.rcMode.create
		return None

	self.status = self.rcMode.status
	self.frozen = self.rcMode.frozen

	if not Freezer(self.svcname).frozen():
		self.freeze = self.rcMode.freeze
	else:
		self.thaw = self.rcMode.thaw
		return None

	# generic actions
	self.start = self.rcMode.start
	self.stop = self.rcMode.stop
	self.startapp = self.rcMode.startapp
	self.stopapp = self.rcMode.stopapp
	self.syncnodes = self.rcMode.syncnodes
	self.syncdrp = self.rcMode.syncdrp

	if self.conf.has_section("fs1") is True or \
	   self.conf.has_section("disk1") is True:
		self.mount = self.rcMode.mount
		self.umount = self.rcMode.umount
	if self.conf.has_section("nfs1") is True:
		self.mountnfs = self.rcMode.mountnfs
		self.umountnfs = self.rcMode.umountnfs
	if self.conf.has_section("ip1") is True:
		self.startip = self.rcMode.startip
		self.stopip = self.rcMode.stopip
	if self.svcmode == 'lxc':
		self.startlxc = self.rcMode.startlxc
		self.stoplxc = self.rcMode.stoplxc

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

	def add_ip(self, ipname, ipdev):
		log = logging.getLogger('INIT')
		ip = self.rcMode.Ip(ipname, ipdev)
		if ip is None:
			log.error("initialization failed for ip (%s@%s)" %
				 (ipname, ipdev))
			return 1
		log.debug("initialization succeeded for ip (%s@%s)" %
			 (ipname, ipdev))
		self.ips.append(ip)

	def add_filesystem(self, dev, mnt, type, mnt_opt):
		log = logging.getLogger('INIT')
		fs = self.rcMode.Filesystem(dev, mnt, type, mnt_opt)
		if fs is None:
			log.error("initialization failed for fs (%s %s %s %s)" %
				 (dev, mnt, type, mnt_opt))
			return 1
		log.debug("initialization succeeded for fs (%s %s %s %s)" %
			 (dev, mnt, type, mnt_opt))
		self.filesystems.append(fs)

	def __init__(self, name):
		#
		# file tree abstraction
		#
		self.svcname = name
		rcEnv.pathsvc = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
		rcEnv.pathbin = os.path.join(rcEnv.pathsvc, 'bin')
		rcEnv.pathetc = os.path.join(rcEnv.pathsvc, 'etc')
		rcEnv.pathlib = os.path.join(rcEnv.pathsvc, 'lib')
		rcEnv.pathlog = os.path.join(rcEnv.pathsvc, 'log')
		rcEnv.pathtmp = os.path.join(rcEnv.pathsvc, 'tmp')
		rcEnv.pathvar = os.path.join(rcEnv.pathsvc, 'var')
		rcEnv.logfile = os.path.join(rcEnv.pathlog, self.svcname) + '.log'
		rcEnv.svcconf = os.path.join(rcEnv.pathetc, self.svcname) + '.env'
		rcEnv.svcinitd = os.path.join(rcEnv.pathetc, self.svcname) + '.d'
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
		log.debug('service name = ' + self.svcname)
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
		self.svcmode = "hosted"
		if os.path.isfile(rcEnv.svcconf):
			self.conf = ConfigParser.RawConfigParser()
			self.conf.read(rcEnv.svcconf)
			if self.conf.has_option("default", "mode"):
				self.svcmode = self.conf.get("default", "mode")

		#
		# dynamically import the action class matching the service mode
		#
		log.debug('service mode = ' + self.svcmode)
		self.rcMode = __import__(svcmode_mod_name(self.svcmode), globals(), locals(), [], -1)

		if install_actions(self) != 0: return None

		self.ips = []
		self.filesystems = []
		add_ips(self)
		add_filesystems(self)

