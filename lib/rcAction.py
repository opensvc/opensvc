import rcLogger

class do:
	def start(self):
		self.startip()
		self.mount()
		self.startapp()

	def stop(self):
		self.stopapp()
		self.umount()
		self.stopip()

	def startip(self):
		log = rcLogger.logger('STARTIP')
		log.info('enter')

	def mount(self):
		log = rcLogger.logger('MOUNT')
		log.info('enter')

	def startapp(self):
		log = rcLogger.logger('STARTAPP')
		log.info('enter')

	def stopip(self):
		log = rcLogger.logger('STOPIP')
		log.info('enter')

	def umount(self):
		log = rcLogger.logger('UMOUNT')
		log.info('enter')

	def stopapp(self):
		log = rcLogger.logger('STOPAPP')
		log.info('enter')

	def create(self):
		log = rcLogger.logger('CREATE')
		log.info('enter')

