import logging
import os
import glob
import re
from datetime import datetime
from subprocess import *

from rcGlobalEnv import *
import rcIP
import rcFilesystem
import rcIfconfig

def instanciate_ip(section):
	log = logging.getLogger('INIT')
	ipname = rcEnv.conf.get(section, "ipname")
	ipdev = rcEnv.conf.get(section, "ipdev")
	ip = HostedIp(ipname, ipdev)
	if ip is None:
		log.error("initialization failed for %s (%s@%s)" %
			 (section, ipname, ipdev))
		return 1
	log.debug("initialization succeeded for %s (%s@%s)" %
		 (section, ipname, ipdev))
	rcEnv.ips.append(ip)

def instanciate_ips():
	rcEnv.ips = []
	for s in rcEnv.conf.sections():
		if 'ip' in s:
			instanciate_ip(s)

def instanciate_filesystem(section):
	log = logging.getLogger('INIT')
	dev = rcEnv.conf.get(section, "dev")
	mnt = rcEnv.conf.get(section, "mnt")
	type = rcEnv.conf.get(section, "type")
	mnt_opt = rcEnv.conf.get(section, "mnt_opt")
	fs = rcFilesystem.HostedFilesystem(dev, mnt, type, mnt_opt)
	if fs is None:
		log.error("initialization failed for %s (%s %s %s %s)" %
			 (section, dev, mnt, type, mnt_opt))
		return 1
	log.debug("initialization succeeded for %s (%s %s %s %s)" %
		 (section, dev, mnt, type, mnt_opt))
	rcEnv.filesystems.append(fs)

def instanciate_filesystems():
	rcEnv.filesystems = []
	for s in rcEnv.conf.sections():
		if 'fs' in s:
			instanciate_filesystem(s)

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

class HostedIp(rcIP.ip):
	def __init__(self, name, dev):
		log = logging.getLogger('INIT')
		rcIP.ip.__init__(self, name, dev)

	def is_up(self):
		if rcEnv.ifconfig.has_param("ipaddr", self.addr) is not None:
			return 0
		return 1

	def start(self):
		log = logging.getLogger('STARTIP')

		#
		# fetch ifconfig information
		#
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

		if self.is_up() == 0:
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
		if self.is_up() != 0:
			log.info("%s is already down on %s" % (self.addr, self.dev))
			return 0
		stacked_dev = get_stacked_dev(self.dev, self.addr, log)
		log.info("ifconfig "+stacked_dev+" down")
		if os.spawnlp(os.P_WAIT, 'ifconfig', 'ifconfig', stacked_dev, 'down') != 0:
			log.error("failed")
			return 1
		return 0

def _start():
	if _startip() != 0: return 1
	if _mount() != 0: return 1
	if _startapp() != 0: return 1
	return 0

def _stop():
	if _stopapp() != 0: return 1
	if _umount() != 0: return 1
	if _stopip() != 0: return 1
	return 0

def _syncnodes():
	log = logging.getLogger('SYNCNODES')
	return 0

def _syncdrp():
	log = logging.getLogger('SYNCDRP')
	return 0

def _startip():
	log = logging.getLogger('STARTIP')
	for ip in rcEnv.ips:
		if ip.start() != 0: return 1
	return 0

def _stopip():
	log = logging.getLogger('STOPIP')
	for ip in rcEnv.ips:
		if ip.stop() != 0: return 1
	return 0

def _mount():
	log = logging.getLogger('MOUNT')
	for f in rcEnv.filesystems:
		if f.start() != 0: return 1
	return 0

def _umount():
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

def _startapp():
	for name in glob.glob(rcEnv.svcinitd + '/S*'):
		app(name, 'start')
	return 0

def _stopapp():
	for name in glob.glob(rcEnv.svcinitd + '/K*'):
		app(name, 'stop')
	return 0

def _create():
	log = logging.getLogger('CREATE')
	return 0

class hosted_do:
	start = _start
	stop = _stop
	startip = _startip
	stopip = _stopip
	mount = _mount
	umount = _umount
	startapp = _startapp
	stopapp = _stopapp
	syncnodes = _syncnodes
	syncdrp = _syncdrp
	create = _create

	def __init__(self):
		if rcEnv.conf is None:
			self.create = _create

		# generic actions
		self.start = _start
		self.stop = _stop
		self.startapp = _startapp
		self.stopapp = _stopapp
		self.syncnodes = _syncnodes
		self.syncdrp = _syncdrp

		if rcEnv.conf.has_section("fs1") is True or \
		   rcEnv.conf.has_section("disk1") is True:
                        self.mount = _mount
                        self.umount = _umount
			instanciate_filesystems()
		if rcEnv.conf.has_section("nfs1") is True:
                        self.mountnfs = _mountnfs
                        self.umountnfs = _umountnfs
		if rcEnv.conf.has_section("ip1") is True:
                        self.startip = _startip
                        self.stopip = _stopip
			instanciate_ips()

