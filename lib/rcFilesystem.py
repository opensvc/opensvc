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

import rcStatus
import rcMounts

class Filesystem:
	def __init__(self, dev, mnt, type, mnt_opt):
		self.dev = dev
		self.mnt = mnt
		self.type = type
		self.mnt_opt = mnt_opt
                self.Mounts = rcMounts.Mounts()

	def is_up(self):
		if self.Mounts.has_mount(self.dev, self.mnt) != 0:
			return False
		return True

	def status(self):
		if self.is_up() is True:
			return rcStatus.UP
		else:
			return rcStatus.DOWN

	def start(self):
		log = logging.getLogger('MOUNT')
                if self.is_up() is True:
                        log.info("fs(%s %s) is already mounted"%
				(self.dev, self.mnt))
                        return 0
		if not os.path.exists(self.mnt):
			os.mkdir(self.mnt, 0755)
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



