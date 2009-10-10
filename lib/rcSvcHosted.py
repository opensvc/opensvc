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
import os
import re
from datetime import datetime
from subprocess import *

from rcGlobalEnv import *
from rcFreeze import Freezer
import rcStatus
import rcIp
import rcFilesystem
import rcIfconfig
import rcLinuxLoop

rcLvm = __import__('rc'+rcEnv.sysname+'Lvm', globals(), locals(), [], -1)

class Ip(rcIp.Ip):
	def __init__(self, svc, name, dev):
		rcIp.Ip.__init__(self, name, dev)

class Filesystem(rcFilesystem.Filesystem):
	def __init__(self, dev, mnt, type, mnt_opt, optional):
		rcFilesystem.Filesystem.__init__(self, dev, mnt, type, mnt_opt, optional)

class Loop(rcLinuxLoop.Loop):
	def __init__(self, name):
		rcLinuxLoop.Loop.__init__(self, name)

class Vg(rcLvm.Vg):
	def __init__(self, name, optional=False):
		rcLvm.Vg.__init__(self, name, optional)

def start(self):
	"""Combo action: startip => diskstart => startapp
	"""
	if startip(self) != 0: return 1
	if diskstart(self) != 0: return 1
	if startapp(self) != 0: return 1
	return 0

def stop(self):
	"""Combo action: stopapp => diskstop => stopip
	"""
	if stopapp(self) != 0: return 1
	if diskstop(self) != 0: return 1
	if stopip(self) != 0: return 1
	return 0

def diskstart(self):
	"""Combo action: startloop => startvg => mount
	"""
	if startloop(self) != 0: return 1
	if startvg(self) != 0: return 1
	if mount(self) != 0: return 1
	return 0

def diskstop(self):
	"""Combo action: umount => stopvg => stoploop
	"""
	if umount(self) != 0: return 1
	if stopvg(self) != 0: return 1
	if stoploop(self) != 0: return 1
	return 0

def syncnodes(self):
	log = logging.getLogger('SYNCNODES')
	return 0

def syncdrp(self):
	log = logging.getLogger('SYNCDRP')
	return 0

def startip(self):
	log = logging.getLogger('STARTIP')
	for r in self.ips:
		if r.start() != 0: return 1
	return 0

def stopip(self):
	log = logging.getLogger('STOPIP')
	for r in self.ips:
		if r.stop() != 0: return 1
	return 0

def mount(self):
	log = logging.getLogger('MOUNT')
	for r in self.filesystems:
		if r.start() != 0: return 1
	return 0

def umount(self):
	log = logging.getLogger('UMOUNT')
	for r in self.filesystems:
		if r.stop() != 0: return 1
	return 0

def startloop(self):
	log = logging.getLogger('STARTLOOP')
	for r in self.loops:
		if r.start() != 0: return 1
	return 0

def stoploop(self):
	log = logging.getLogger('STOPLOOP')
	for r in self.loops:
		if r.stop() != 0: return 1
	return 0

def startvg(self):
	log = logging.getLogger('STARTVG')
	for r in self.volumegroups:
		if r.start() != 0: return 1
	return 0

def stopvg(self):
	log = logging.getLogger('STOPVG')
	for r in self.volumegroups:
		if r.stop() != 0: return 1
	return 0

def startapp(self):
	return self.apps.start()

def stopapp(self):
	return self.apps.stop()

def create(self):
	log = logging.getLogger('CREATE')
	return 0

def status(self, verbose=False):
	status = rcStatus.Status()
	for r in self.ips:
		status.add(r.status(verbose))
	for r in self.filesystems:
		status.add(r.status(verbose))
	for r in self.volumegroups:
		status.add(r.status(verbose))
	for r in self.loops:
		status.add(r.status(verbose))
	if (verbose): rcStatus.print_status("global", (status.status))
	return status.status

def freeze(self):
	f = Freezer(self.svcname)
	f.freeze()

def thaw(self):
	f = Freezer(self.svcname)
	f.thaw()

def frozen(self):
	f = Freezer(self.svcname)
	print str(f.frozen())
	return f.frozen()

