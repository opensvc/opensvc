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
class ip:
	def __init__(self, name, device):
		self.name = name
		self.device = device
		self.devstate = ''
		self.addr = ''
		self.broadcast = ''
		self.mtu = ''
		self.gateway = ''
	def show(self):
		print "name = " + self.name
		print "device = " + self.device
		print "device state = " + self.devstate
		print "addr = " + self.addr
		print "broadcast = " + self.broadcast
		print "mtu = " + self.mtu
		print "gateway = " + self.gateway
	def set_devstate(self, x):
		self.devstate = x
	def set_addr(self, x):
		self.addr = x
	def set_broadcast(self, x):
		self.broadcast = x
	def set_mtu(self, x):
		self.mtu = x
	def set_gateway(self, x):
		self.gateway = x
