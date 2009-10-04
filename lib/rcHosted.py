import logging
import os
import glob
import re
from datetime import datetime
from subprocess import *

from rcGlobalEnv import *
from rcFreeze import Freezer
import rcStatus
import rcIP
import rcFilesystem
import rcIfconfig

def next_stacked_dev(dev):
	"""Return the first available interfaceX:Y on  interfaceX
	"""
	i = 0
	while True:
		stacked_dev = dev+':'+str(i)
		if not rcEnv.ifconfig.has_interface(stacked_dev):
			return stacked_dev
			break
		i = i + 1

def get_stacked_dev(dev, addr, log):
	"""Upon start, a new interfaceX:Y will have to be assigned.
	Upon stop, the currently assigned interfaceX:Y will have to be
	found for ifconfig down
	"""
	stacked_intf = rcEnv.ifconfig.has_param("ipaddr", addr)
	if stacked_intf is not None:
		if dev not in stacked_intf.name:
			log.error("%s is plumbed but not on %s" % (addr, dev))
			return
		stacked_dev = stacked_intf.name
		log.debug("found matching stacked device %s" % stacked_dev)
	else:
		stacked_dev = next_stacked_dev(dev)
		log.debug("allocate new stacked device %s" % stacked_dev)
	return stacked_dev

class Ip(rcIP.ip):
	def __init__(self, name, dev):
		log = logging.getLogger('INIT')
		rcIP.ip.__init__(self, name, dev)

	def is_up(self):
		rcEnv.ifconfig = rcIfconfig.ifconfig()
		rcEnv.ifconfig.interface(self.dev).show()

		if rcEnv.ifconfig.has_param("ipaddr", self.addr) is not None:
			return True
		return False

	def status(self):
		if self.is_up() is True:
			return rcStatus.UP
		else:
			return rcStatus.DOWN

	def start(self):
		log = logging.getLogger('STARTIP')
		rcEnv.ifconfig = rcIfconfig.ifconfig()
		rcEnv.ifconfig.interface(self.dev).show()

		if not rcEnv.ifconfig.interface(self.dev).flag_up:
			log.error("Device %s is not up. Cannot stack over it." % self.dev)
			return None

		#
		# get netmask from ipdev
		#
		self.mask = rcEnv.ifconfig.interface(self.dev).mask
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
		rcEnv.ifconfig = rcIfconfig.ifconfig()
		rcEnv.ifconfig.interface(self.dev).show()
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

def start():
	if startip() != 0: return 1
	if mount() != 0: return 1
	if startapp() != 0: return 1
	return 0

def stop():
	if stopapp() != 0: return 1
	if umount() != 0: return 1
	if stopip() != 0: return 1
	return 0

def syncnodes():
	log = logging.getLogger('SYNCNODES')
	return 0

def syncdrp():
	log = logging.getLogger('SYNCDRP')
	return 0

def startip():
	log = logging.getLogger('STARTIP')
	for ip in rcEnv.ips:
		if ip.start() != 0: return 1
	return 0

def stopip():
	log = logging.getLogger('STOPIP')
	for ip in rcEnv.ips:
		if ip.stop() != 0: return 1
	return 0

def mount():
	log = logging.getLogger('MOUNT')
	for f in rcEnv.filesystems:
		if f.start() != 0: return 1
	return 0

def umount():
	log = logging.getLogger('UMOUNT')
	for f in rcEnv.filesystems:
		if f.stop() != 0: return 1
	return 0

def app(name, action):
	if action == 'start':
		log = logging.getLogger('STARTAPP')
	else:
		log = logging.getLogger('STOPAPP')

	log.info('spawn: %s %s' % (name, action))
	outf = '/var/tmp/svc_'+rcEnv.svcname+'_'+os.path.basename(name)+'.log'
	f = open(outf, 'a')
	t = datetime.now()
	f.write(str(t))
	p = Popen([name, action], stdout=PIPE)
	ret = p.wait()
	f.write(p.communicate()[0])
	len = datetime.now() - t
	log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
	f.close()

def startapp():
	for name in glob.glob(os.path.join(rcEnv.svcinitd, 'S*')):
		app(name, 'start')
	return 0

def stopapp():
	for name in glob.glob(os.path.join(rcEnv.svcinitd, 'K*')):
		app(name, 'stop')
	return 0

def create():
	log = logging.getLogger('CREATE')
	return 0

def status():
	status = rcStatus.Status()
	for ip in rcEnv.ips:
		print "ip %s@%s: %s" % (ip.name, ip.dev, status.str(ip.status()))
		status.add(ip.status())
	for fs in rcEnv.filesystems:
		print "fs %s@%s: %s" % (fs.dev, fs.mnt, status.str(fs.status()))
		status.add(fs.status())
	print "global: %s" % status.str(status.status)

def freeze ():
	f = Freezer(rcEnv.svcname)
	f.freeze()

def thaw ():
	f = Freezer(rcEnv.svcname)
	f.thaw()

def frozen ():
	f = Freezer(rcEnv.svcname)
	print str(f.frozen())
	return f.frozen()

