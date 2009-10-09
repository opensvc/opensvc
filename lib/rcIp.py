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
import socket
import os
from subprocess import *

from rcLogger import *
from rcUtilities import process_call_argv
import rcIfconfig
import rcStatus

def next_stacked_dev(dev, ifconfig):
	"""Return the first available interfaceX:Y on  interfaceX
	"""
	i = 0
	while True:
		stacked_dev = dev+':'+str(i)
		if not ifconfig.has_interface(stacked_dev):
			return stacked_dev
			break
		i = i + 1

def get_stacked_dev(dev, addr, log):
	"""Upon start, a new interfaceX:Y will have to be assigned.
	Upon stop, the currently assigned interfaceX:Y will have to be
	found for ifconfig down
	"""
	ifconfig = rcIfconfig.ifconfig()
	stacked_intf = ifconfig.has_param("ipaddr", addr)
	if stacked_intf is not None:
		if dev not in stacked_intf.name:
			log.error("%s is plumbed but not on %s" % (addr, dev))
			return
		stacked_dev = stacked_intf.name
		log.debug("found matching stacked device %s" % stacked_dev)
	else:
		stacked_dev = next_stacked_dev(dev, ifconfig)
		log.debug("allocate new stacked device %s" % stacked_dev)
	return stacked_dev

class IpDevDown(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)

class IpConflict(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)

class Ip:
	def is_alive(self):
		log = logging.getLogger('Ip.is_alive')
		count=1
		timeout=5
		cmd = ['ping', '-c', repr(count), '-W', repr(timeout), self.addr]
		log.debug(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		if ret == 0:
			return True
		return False

	def is_up(self):
		log = logging.getLogger('Ip.is_up')
		ifconfig = rcIfconfig.ifconfig()
		if ifconfig.has_param("ipaddr", self.addr) is not None:
			log.debug("%s@%s is up" % (self.addr, self.dev))
			return True
		log.debug("%s@%s is down" % (self.addr, self.dev))
		return False

	def status(self):
		if self.is_up() is True:
			return rcStatus.UP
		else:
			return rcStatus.DOWN

	def allow_start(self):
		log = logging.getLogger('STARTIP')
		ifconfig = rcIfconfig.ifconfig()
		if not ifconfig.interface(self.dev).flag_up:
			log.error("Interface %s is not up. Cannot stack over it." % self.dev)
			raise IpDevDown(self.dev)
		if self.is_up() is True:
			log.info("%s is already up on %s" % (self.addr, self.dev))
			return False
		if self.is_alive():
			log.error("%s is already up on another host" % (self.addr))
			raise IpConflict(self.addr)
		return True

	def start(self):
		log = logging.getLogger('STARTIP')
		try:
			if not self.allow_start():
				return 0
		except IpConflict, IpDevDown:
			return 1
		log.debug('pre-checks passed')

		ifconfig = rcIfconfig.ifconfig()
		self.mask = ifconfig.interface(self.dev).mask
		if self.mask == '':
			log.error("No netmask set on parent interface %s" % self.dev)
			return None
		if self.mask == '':
			log.error("No netmask found. Abort")
			return 1
		stacked_dev = get_stacked_dev(self.dev, self.addr, log)
		cmd = ['ifconfig', stacked_dev, self.addr, 'netmask', self.mask, 'up']
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		if ret != 0:
			log.error("failed")
			return 1
		return 0

	def stop(self):
		log = logging.getLogger('STOPIP')
		if self.is_up() is False:
			log.info("%s is already down on %s" % (self.addr, self.dev))
			return 0
		stacked_dev = get_stacked_dev(self.dev, self.addr, log)
		cmd = ['ifconfig', stacked_dev, 'down']
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		if ret != 0:
			log.error("failed")
			return 1
		return 0

	def __init__(self, name, dev):
		self.name = name
		self.dev = dev
		self.addr = socket.gethostbyname(name)
		log = logging.getLogger('INIT')


