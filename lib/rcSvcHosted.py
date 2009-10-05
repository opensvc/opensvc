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

class Ip(rcIp.Ip):
	def __init__(self, name, dev):
		rcIp.Ip.__init__(self, name, dev)

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

