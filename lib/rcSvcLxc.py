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
import logging
from datetime import datetime
from subprocess import *

from rcGlobalEnv import *
from rcFreeze import Freezer
import rcIp
import rcFilesystem
import rcSvcHosted
import rcLxc

def start(self):
	startip(self)
	mount(self)
	startlxc(self)
	startapp(self)

def stop(self):
	stopapp(self)
	stoplxc(self)
	umount(self)
	stopip(self)

def startip(self):
	"""startip is a noop for LXC : ips are plumbed on container start-up
	Keep the checks though to avoid starting the container if ips are plumbed
	on some other host
	"""
	return rcSvcHosted.startip(self)

def stopip(self):
	"""stopip is a noop for LXC : ips are unplumbed on container stop
	"""
	return rcSvcHosted.stopip(self)

def mount(self):
	log = logging.getLogger('MOUNT')
	return rcSvcHosted.mount(self)

def umount(self):
	log = logging.getLogger('UMOUNT')
	return rcSvcHosted.umount(self)

def startlxc(self):
	log = logging.getLogger('STARTLXC')
	return rcLxc.Lxc(self.svcname).start()

def stoplxc(self):
	log = logging.getLogger('STOPLXC')
	return rcLxc.Lxc(self.svcname).stop()

def startapp(self):
	log = logging.getLogger('STARTAPP')
	log.info("TODO")
	return 0

def stopapp(self):
	log = logging.getLogger('STOPAPP')
	log.info("TODO")
	return 0

def configure(self):
	log = logging.getLogger('CONFIGURE')
	log.info("TODO")
	return 0

def syncnodes(self):
	log = logging.getLogger('SYNCNODES')
	log.info("TODO")
	return 0

def syncdrp(self):
	log = logging.getLogger('SYNCDRP')
	log.info("TODO")
	return 0

def create(self):
	log = logging.getLogger('CREATE')
	log.info("TODO")
	return 0

def status(self):
	log = logging.getLogger('STATUS')
	log.info("TODO")
	return 0

def freeze(self):
	return rcSvcHosted.freeze(self)

def thaw(self):
	return rcSvcHosted.thaw(self)

def frozen(self):
	return rcSvcHosted.frozen(self)

class Ip(rcIp.Ip):
	def is_up(self):
		if self.is_alive():
			return True
		return False

	def stop(self):
		pass

	def start(self):
		log = logging.getLogger('STARTIP')
		try:
			if not self.allow_start():
				return 0
		except rcIp.IpConflict, rcIp.IpDevDown:
			return 1

	def __init__(self, name, dev):
		rcIp.Ip.__init__(self, name, dev)

class Filesystem(rcFilesystem.Filesystem):
	def __init__(self, dev, mnt, type, mnt_opt):
		rcFilesystem.Filesystem.__init__(self, dev, mnt, type, mnt_opt)

