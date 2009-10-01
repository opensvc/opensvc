import logging
from datetime import datetime
from subprocess import *

from rcGlobalEnv import *
import rcIP
import rcHosted

def lxc(action):
	if action == 'start':
                log = logging.getLogger('STARTLXC')
        else:
                log = logging.getLogger('STOPLXC')

	log.info('spawn: lxc-%s -b -n %s' % (action, rcEnv.svcname))
	outf = '/var/tmp/svc_'+rcEnv.svcname+'_lxc_'+action+'.log'
	cmd = ['lxc-start -b -n ', rcEnv.svcname]
        f = open(outf, 'a')
        t = datetime.now()
        f.write(str(t))
        p = Popen(cmd, stdout=PIPE)
        ret = p.wait()
        f.write(p.communicate()[0])
        len = datetime.now() - t
        log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
        f.close()

def _start():
	startip()
	mount()
	startlxc()
	startapp()

def _stop():
	stopapp()
	stoplxc()
	umount()
	stopip()

def _startip():
	"""startip is a noop for LXC : ips are plumbed on container start-up
	"""
	log = logging.getLogger('STARTIP')
	log.info('no-op')

def _stopip():
	"""stopip is a noop for LXC : ips are unplumbed on container stop
	"""
	log = logging.getLogger('STOPIP')
	log.info('no-op')

def _mount():
	log = logging.getLogger('MOUNT')
	if rcHosted._mount() != 0:
		return 1
	return 0

def _umount():
	log = logging.getLogger('UMOUNT')
	if rcHosted._umount() != 0:
		return 1
	return 0

def _startlxc():
	log = logging.getLogger('STARTLXC')
	if lxc(start) != 0:
		return 1
	return 0

def _stoplxc():
	log = logging.getLogger('STOPLXC')
	if lxc(stop) != 0:
		return 1
	return 0

def _startapp():
	log = logging.getLogger('STARTAPP')
	return 0

def _stopapp():
	log = logging.getLogger('STOPAPP')
	return 0

def _configure():
	log = logging.getLogger('CONFIGURE')
	return 0

def _syncnodes():
	log = logging.getLogger('SYNCNODES')
	return 0

def _syncdrp():
	log = logging.getLogger('SYNCDRP')
	return 0

def _create(self):
	log = logging.getLogger('CREATE')
	return 0

class lxc_do(rcHosted.hosted_do):
	"""
	start = _start
	stop = _stop
	startlxc = _startlxc
	stoplxc = _stoplxc
	startip = _startip
	stopip = _stopip
	mount = _mount
	umount = _umount
	startapp = _startapp
	stopapp = _stopapp
	configure = _configure
	syncnodes = _syncnodes
	syncdrp = _syncdrp
	create = _create

	def __init__(self):
		if rcEnv.conf is None:
			self.create = _create
			return

		# generic actions
		self.start = _start
		self.stop = _stop
		self.startapp = _startapp
		self.stopapp = _stopapp
		self.syncnodes = _syncnodes
		self.syncdrp = _syncdrp
		self.startlxc = _startlxc
		self.stoplxc = _stoplxc

		if rcEnv.conf.has_section("fs1") is True or \
		   rcEnv.conf.has_section("disk1") is True:
			self.mount = _mount
			self.umount = _umount
		if rcEnv.conf.has_section("nfs1") is True:
			self.mountnfs = _mountnfs
			self.umountnfs = _umountnfs
		if rcEnv.conf.has_section("ip1") is True:
			self.startip = _startip
			self.stopip = _stopip
	"""
	def __init__(self):
		rcHosted.hosted_do.__init__(self)
		self.startlxc = _startlxc
		self.stoplxc = _stoplxc

