import rcIP
import rcLogger

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
	log = rcLogger.logger('STARTIP')
	log.info('done')

def mount():
	log = rcLogger.logger('MOUNT')
	log.info('done')

def startapp():
	log = rcLogger.logger('STARTAPP')
	log.info('done')

def stopip():
	log = rcLogger.logger('STOPIP')
	log.info('done')

def umount():
	log = rcLogger.logger('UMOUNT')
	log.info('done')

def stopapp():
	log = rcLogger.logger('STOPAPP')
	log.info('done')

def configure():
	log = rcLogger.logger('CONFIGURE')
	log.info('done')

def syncnodes():
	log = rcLogger.logger('SYNCNODES')
	log.info('done')

def syncdrp():
	log = rcLogger.logger('SYNCDRP')
	log.info('done')

