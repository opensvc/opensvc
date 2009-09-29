import rcIP
import logging

def start():
	startip()
	mount()
	startapp()

def stop():
	stopapp()
	umount()
	stopip()

def startip():
	"""startip is a noop for LXC : ips are plumbed at container start-up
	"""
	log = logging.getLogger('STARTIP')
	log.info('done')

def mount():
	log = logging.getLogger('MOUNT')
	log.info('done')

def startapp():
	log = logging.getLogger('STARTAPP')
	log.info('done')

def stopip():
	log = logging.getLogger('STOPIP')
	log.info('done')

def umount():
	log = logging.getLogger('UMOUNT')
	log.info('done')

def stopapp():
	log = logging.getLogger('STOPAPP')
	log.info('done')

def configure():
	log = logging.getLogger('CONFIGURE')
	log.info('done')

def syncnodes():
	log = logging.getLogger('SYNCNODES')
	log.info('done')

def syncdrp():
	log = logging.getLogger('SYNCDRP')
	log.info('done')

