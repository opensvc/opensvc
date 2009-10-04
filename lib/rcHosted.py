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
import glob
import re
from datetime import datetime
from subprocess import *

from rcGlobalEnv import *
from rcFreeze import Freezer
import rcStatus
import rcIp
import rcFilesystem
import rcIfconfig

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

class Ip(rcIp.ip):
	def __init__(self, name, dev):
		log = logging.getLogger('INIT')
		rcIp.ip.__init__(self, name, dev)

	def is_up(self):
		ifconfig = rcIfconfig.ifconfig()
		if ifconfig.has_param("ipaddr", self.addr) is not None:
			return True
		return False

	def status(self):
		if self.is_up() is True:
			return rcStatus.UP
		else:
			return rcStatus.DOWN

	def start(self):
		log = logging.getLogger('STARTIP')
		ifconfig = rcIfconfig.ifconfig()
		if not ifconfig.interface(self.dev).flag_up:
			log.error("Device %s is not up. Cannot stack over it." % self.dev)
			return None

		#
		# get netmask from ipdev
		#
		self.mask = ifconfig.interface(self.dev).mask
		if self.mask == '':
			log.error("No netmask set on parent interface %s" % self.dev)
			return None

		if self.is_up() is True:
			log.info("%s is already up on %s" % (self.addr, self.dev))
			return 0
		if self.mask == '':
			log.error("No netmask found. Abort")
			return 1
		stacked_dev = get_stacked_dev(self.dev, self.addr, log)
		log.info("ifconfig "+stacked_dev+" "+self.addr+" netmask "+self.mask+" up")
		if os.spawnlp(os.P_WAIT, 'ifconfig', 'ifconfig', stacked_dev, self.addr, 'netmask', self.mask, 'up') != 0:
			log.error("failed")
			return 1
		return 0

	def stop(self):
		log = logging.getLogger('STOPIP')
		if self.is_up() is False:
			log.info("%s is already down on %s" % (self.addr, self.dev))
			return 0
		stacked_dev = get_stacked_dev(self.dev, self.addr, log)
		log.info("ifconfig "+stacked_dev+" down")
		if os.spawnlp(os.P_WAIT, 'ifconfig', 'ifconfig', stacked_dev, 'down') != 0:
			log.error("failed")
			return 1
		return 0

class Filesystem(rcFilesystem.Filesystem):
	def __init__(self, dev, mnt, type, mnt_opt):
		rcFilesystem.Filesystem.__init__(self, dev, mnt, type, mnt_opt)

def start(self):
	if startip(self) != 0: return 1
	if mount(self) != 0: return 1
	if startapp(self) != 0: return 1
	return 0

def stop(self):
	if stopapp(self) != 0: return 1
	if umount(self) != 0: return 1
	if stopip(self) != 0: return 1
	return 0

def syncnodes(self):
	log = logging.getLogger('SYNCNODES')
	return 0

def syncdrp(self):
	log = logging.getLogger('SYNCDRP')
	return 0

def startip(self):
	log = logging.getLogger('STARTIP')
	for ip in self.ips:
		if ip.start() != 0: return 1
	return 0

def stopip(self):
	log = logging.getLogger('STOPIP')
	for ip in self.ips:
		if ip.stop() != 0: return 1
	return 0

def mount(self):
	log = logging.getLogger('MOUNT')
	for f in self.filesystems:
		if f.start() != 0: return 1
	return 0

def umount(self):
	log = logging.getLogger('UMOUNT')
	for f in self.filesystems:
		if f.stop() != 0: return 1
	return 0

def app(self, name, action):
	if action == 'start':
		log = logging.getLogger('STARTAPP')
	else:
		log = logging.getLogger('STOPAPP')

	log.info('spawn: %s %s' % (name, action))
	outf = '/var/tmp/svc_'+self.svcname+'_'+os.path.basename(name)+'.log'
	f = open(outf, 'a')
	t = datetime.now()
	f.write(str(t))
	p = Popen([name, action], stdout=PIPE)
	ret = p.wait()
	f.write(p.communicate()[0])
	len = datetime.now() - t
	log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
	f.close()

def startapp(self):
	for name in glob.glob(os.path.join(rcEnv.svcinitd, 'S*')):
		app(self, name, 'start')
	return 0

def stopapp(self):
	for name in glob.glob(os.path.join(rcEnv.svcinitd, 'K*')):
		app(self, name, 'stop')
	return 0

def create(self):
	log = logging.getLogger('CREATE')
	return 0

def status(self):
	status = rcStatus.Status()
	for ip in self.ips:
		print "ip %s@%s: %s" % (ip.name, ip.dev, status.str(ip.status()))
		status.add(ip.status())
	for fs in self.filesystems:
		print "fs %s@%s: %s" % (fs.dev, fs.mnt, status.str(fs.status()))
		status.add(fs.status())
	print "global: %s" % status.str(status.status)

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

