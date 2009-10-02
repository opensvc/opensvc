import logging
import os

import rcStatus
import rcMounts
from rcGlobalEnv import *

class HostedFilesystem:
	def __init__(self, dev, mnt, type, mnt_opt):
		log = logging.getLogger('INIT')
		self.dev = dev
		self.mnt = mnt
		self.type = type
		self.mnt_opt = mnt_opt

                rcEnv.Mounts = rcMounts.Mounts()
                m = rcEnv.Mounts.mount(dev, mnt)
		if m is not None:
			m.show()

	def is_up(self):
		if rcEnv.Mounts.has_mount(self.dev, self.mnt) != 0:
			return False
		return True

	def status(self):
		return rcStatus.TODO

	def start(self):
		log = logging.getLogger('MOUNT')
                if self.is_up() is True:
                        log.info("fs(%s %s) is already mounted"%
				(self.dev, self.mnt))
                        return 0
                log.info("mount -t %s -o %s %s %s"%
			(self.type, self.mnt_opt, self.dev, self.mnt))
                if os.spawnlp(os.P_WAIT, 'mount', 'mount', '-t', self.type, '-o', self.mnt_opt, self.dev, self.mnt) != 0:
                        log.error("failed")
                        return 1
                return 0

	def stop(self):
		log = logging.getLogger('UMOUNT')
                if self.is_up() is False:
                        log.info("fs(%s %s) is already umounted"%
				(self.dev, self.mnt))
                        return 0
                log.info("umount %s"% self.mnt)
                if os.spawnlp(os.P_WAIT, 'umount', 'umount', self.mnt) != 0:
                        log.error("failed")
                        return 1
                return 0



