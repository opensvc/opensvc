import rcLogger
from rcGlobalEnv import *
import os
import glob
import rcIP
import re
from subprocess import *

class ip(rcIP.ip):
	def __init__(self, name, dev):
		log = rcLogger.logger('INIT')
		rcIP.ip.__init__(self, name, dev)
		out = Popen(['ifconfig', '-a'], stdout=PIPE).communicate()[0]

		if not re.match('^'+dev+' ', out):
			log.error("Device %s is not up. Cannot stack over it." % dev)
			return None

		i = 0
		while True:
			self.stacked_dev = dev+':'+str(i)
			if not re.match(out, self.stacked_dev):
				self.stacked_dev = dev+':'+str(i)
				break
			i = i + 1
		log.debug("stacked device %s" % self.stacked_dev)

		#
		# detect netmask on ipdev
		#
		log.error("TODO: netmask detection")
		self.mask = "255.255.255.0"

	def start(self):
		log = rcLogger.logger('STARTIP')
		log.info("ifconfig "+self.stacked_dev+" "+self.addr+" netmask "+self.mask+" up")
		os.spawnlp(os.P_WAIT, 'ifconfig', 'ifconfig', self.stacked_dev, self.addr, 'netmask', self.mask, 'up')

	def stop(self):
		log = rcLogger.logger('STOPIP')
		log.info("ifconfig "+self.stacked_dev+" down")
		os.spawnlp(os.P_WAIT, 'ifconfig', 'ifconfig', self.stacked_dev, 'down')

def start():
	self.startip()
	self.mount()
	self.startapp()

def stop():
	self.stopapp()
	self.umount()
	self.stopip()

def syncnodes():
	log = rcLogger.logger('SYNCNODES')
	log.info('enter')

def syncdrp():
	log = rcLogger.logger('SYNCDRP')
	log.info('enter')

def startip():
	log = rcLogger.logger('STARTIP')
	log.info('enter')
	for ip in rcEnv.ips:
		ip.start()

def mount():
	log = rcLogger.logger('MOUNT')
	log.info('enter')

def startapp():
	log = rcLogger.logger('STARTAPP')
	log.info('enter')
	for name in glob.glob(rcEnv.svcinitd + '/S*'):
		log.info('spawn: %s start' % name)
		os.spawnlp(os.P_NOWAIT, name, name, 'start')

def stopip():
	log = rcLogger.logger('STOPIP')
	log.info('enter')

def umount():
	log = rcLogger.logger('UMOUNT')
	log.info('enter')

def stopapp():
	log = rcLogger.logger('STOPAPP')
	log.info('enter')
	for name in glob.glob(rcEnv.svcinitd + '/K*'):
		log.info('spawn: %s stop' % name)
		os.spawnlp(os.P_NOWAIT, name, name, 'stop')

def create():
	log = rcLogger.logger('CREATE')
	log.info('enter')

