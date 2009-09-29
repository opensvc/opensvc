import logging
from rcGlobalEnv import *
import os
import glob
import rcIP
import rcIfconfig
import re
from datetime import datetime
from subprocess import *

def next_stacked_dev(dev):
	i = 0
	while True:
		stacked_dev = dev+':'+str(i)
		if not rcEnv.ifconfig.has_interface(stacked_dev):
			return stacked_dev
			break
		i = i + 1

class ip(rcIP.ip):
	def __init__(self, name, dev):
		log = logging.getLogger('INIT')
		rcIP.ip.__init__(self, name, dev)

		rcEnv.ifconfig = rcIfconfig.ifconfig()
		rcEnv.ifconfig.interface(dev).show()
		if not rcEnv.ifconfig.interface(dev).flag_up:
			log.error("Device %s is not up. Cannot stack over it." % dev)
			return None

		stacked_intf = rcEnv.ifconfig.has_param("ipaddr", self.addr)
		if stacked_intf is not None:
			self.stacked_dev = stacked_intf.name
		else:
			self.stacked_dev = next_stacked_dev(dev)

		log.debug("stacked device %s" % self.stacked_dev)

		#
		# get netmask from ipdev
		#
		self.mask = rcEnv.ifconfig.interface(dev).mask
		if self.mask == '':
			log.error("No netmask set on parent interface %s" % dev)
			return None

	def is_up(self):
		if rcEnv.ifconfig.has_param("ipaddr", self.addr) is not None:
			return 0
		return 1

	def start(self):
		log = logging.getLogger('STARTIP')
		if self.is_up() == 0:
			log.info("%s is already up on %s" % (self.addr, self.dev))
			return 0
		if self.mask == '':
			log.error("No netmask found. Abort")
			return 1
		log.info("ifconfig "+self.stacked_dev+" "+self.addr+" netmask "+self.mask+" up")
		if os.spawnlp(os.P_WAIT, 'ifconfig', 'ifconfig', self.stacked_dev, self.addr, 'netmask', self.mask, 'up') != 0:
			log.error("failed")
			return 1
		return 0

	def stop(self):
		log = logging.getLogger('STOPIP')
		if self.is_up() != 0:
			log.info("%s is already down on %s" % (self.addr, self.dev))
			return 0
		log.info("ifconfig "+self.stacked_dev+" down")
		if os.spawnlp(os.P_WAIT, 'ifconfig', 'ifconfig', self.stacked_dev, 'down') != 0:
			log.error("failed")
			return 1
		return 0

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
	return 0

def umount():
	log = logging.getLogger('UMOUNT')
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
	p = Popen([name, 'start'], stdout=PIPE)
	ret = p.wait()
	f.write(p.communicate()[0])
	len = datetime.now() - t
	log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
	f.close()

def startapp():
	for name in glob.glob(rcEnv.svcinitd + '/S*'):
		app(name, 'start')
	return 0

def stopapp():
	for name in glob.glob(rcEnv.svcinitd + '/K*'):
		app(name, 'stop')
	return 0

def create():
	log = logging.getLogger('CREATE')
	return 0

