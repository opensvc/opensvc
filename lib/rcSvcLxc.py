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
from freezer import Freezer
import ResIp
import ResFilesystem
import rcSvcHosted
import rcLxc
import rcStatus

def start(self):
	if startip(self) != 0: return 1
	if diskstart(self) != 0: return 1
	if startlxc(self) != 0: return 1
	if startapp(self) != 0: return 1
	return 0

def stop(self):
	if stopapp(self) != 0: return 1
	if stoplxc(self) != 0: return 1
	if diskstop(self) != 0: return 1
	if stopip(self) != 0: return 1
	return 0

def diskstart(self):
	return rcSvcHosted.diskstart(self)

def diskstop(self):
	return rcSvcHosted.diskstop(self)

def startloop(self):
	return rcSvcHosted.startloop(self)

def stoploop(self):
	return rcSvcHosted.stoploop(self)

def startvg(self):
	return rcSvcHosted.startvg(self)

def stopvg(self):
	return rcSvcHosted.stopvg(self)

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
	return self.lxc.start()

def stoplxc(self):
	log = logging.getLogger('STOPLXC')
	return self.lxc.stop()

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

def status(self, verbose=False):
	status = rcStatus.Status()
	status.add(self.lxc.status(verbose))
	for ip in self.ips:
		status.add(ip.status(verbose))
	for fs in self.filesystems:
		status.add(fs.status(verbose))
	for r in self.volumegroups:
		status.add(r.status(verbose))
	for r in self.loops:
		status.add(r.status(verbose))
	if verbose: rcStatus.print_status("global", status.status)
	return status.status

def freeze(self):
	return rcSvcHosted.freeze(self)

def thaw(self):
	return rcSvcHosted.thaw(self)

def frozen(self):
	return rcSvcHosted.frozen(self)

class Ip(ResIp.Ip):
	def is_up(self):
		if self.svc.lxc.is_up() and self.is_alive():
			return True
		return False

	def stop(self):
		pass

	def start(self):
		log = logging.getLogger('STARTIP')
		try:
			if not self.allow_start():
				return 0
		except ResIp.IpConflict, ResIp.IpDevDown:
			return 1
		log.debug('pre-checks passed')
		log.info('nothing to do for lxc containers')
		return 0

	def __init__(self, svc, name, dev):
		self.svc = svc
		ResIp.Ip.__init__(self, name, dev)

class Filesystem(ResFilesystem.Filesystem):
	def __init__(self, dev, mnt, type, mnt_opt, optional):
		ResFilesystem.Filesystem.__init__(self, dev, mnt, type, mnt_opt, optional)

class Lxc(rcLxc.Lxc):
	def __init__(self, svc):
		rcLxc.Lxc.__init__(self, svc)
