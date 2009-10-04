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
from subprocess import *
import logging
from rcGlobalEnv import *

class interface:
	def show(self):
		log = logging.getLogger('INIT')
		log.debug('ifconfig:')
		log.debug(self.name + ' link_encap = ' + self.link_encap)
		log.debug(self.name + ' scope = ' + self.scope)
		log.debug(self.name + ' bcast = ' + self.bcast)
		log.debug(self.name + ' mask = ' + self.mask)
		log.debug(self.name + ' mtu = ' + self.mtu)
		log.debug(self.name + ' ipaddr = ' + self.ipaddr)
		log.debug(self.name + ' ip6addr = ' + self.ip6addr)
		log.debug(self.name + ' hwaddr = ' + self.hwaddr)
		log.debug(self.name + ' flag_up = ' + str(self.flag_up))
		log.debug(self.name + ' flag_broadcast = ' + str(self.flag_broadcast))
		log.debug(self.name + ' flag_running = ' + str(self.flag_running))
		log.debug(self.name + ' flag_multicast = ' + str(self.flag_multicast))
		log.debug(self.name + ' flag_loopback = ' + str(self.flag_loopback))

	def __init__(self, name):
		self.name = name

class ifconfig:
	intf = []

	def add_interface(self, name):
		i = interface(name)
		self.intf.append(i)

	def interface(self, name):
		for i in self.intf:
			if i.name == name:
				return i
		return None

	def has_interface(self, name):
		for i in self.intf:
			if i.name == name:
				return 1
		return 0

	def has_param(self, param, value):
		for i in self.intf:
			if getattr(i, param) == value:
				return i
		return None

	def __init__(self):
		out = Popen(['ifconfig', '-a'], stdout=PIPE).communicate()[0]
		prev = ''
		prevprev = ''
		for w in out.split():
			if w == 'Link':
				i = interface(prev)
				self.intf.append(i)

				# defaults
				i.link_encap = ''
				i.scope = ''
				i.bcast = ''
				i.mask = ''
				i.mtu = ''
				i.ipaddr = ''
				i.ip6addr = ''
				i.hwaddr = ''
				i.flag_up = False
				i.flag_broadcast = False
				i.flag_running = False
				i.flag_multicast = False
				i.flag_loopback = False
			elif 'encap:' in w:
				(null, i.link_encap) = w.split(':')
			elif 'Scope:' in w:
				(null, i.scope) = w.split(':')
			elif 'Bcast:' in w:
				(null, i.bcast) = w.split(':')
			elif 'Mask:' in w:
				(null, i.mask) = w.split(':')
			elif 'MTU:' in w:
				(null, i.mtu) = w.split(':')

			if 'inet' == prev and 'addr:' in w:
				(null, i.ipaddr) = w.split(':')
			if 'inet6' == prevprev and 'addr:' == prev:
				i.ip6addr = w
			if 'HWaddr' == prev:
				i.hwaddr = w
			if 'UP' == w:
				i.flag_up = True
			if 'BROADCAST' == w:
				i.flag_broadcast = True
			if 'RUNNING' == w:
				i.flag_running = True
			if 'MULTICAST' == w:
				i.flag_multicast = True
			if 'LOOPBACK' == w:
				i.flag_loopback = True
				
			prevprev = prev
			prev = w
	
