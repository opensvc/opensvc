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
from rcUtilities import process_call_argv

class Filesystem:
	def __init__(self, dev, mnt, type, mnt_opt, optional=False):
		self.dev = dev
		self.mnt = mnt
		self.type = type
		self.mnt_opt = mnt_opt
		self.optional = optional
                self.Mounts = rcMounts.Mounts()

	def is_up(self):
		if self.Mounts.has_mount(self.dev, self.mnt) != 0:
			return False
		return True

	def status(self, verbose=False):
		if self.is_up() is True:
			status = rcStatus.UP
		else:
			status = rcStatus.DOWN
		if verbose:
			rcStatus.print_status("fs %s@%s" % (self.dev, self.mnt), status)
		return status

	def start(self):
		log = logging.getLogger('MOUNT')
                if self.is_up() is True:
                        log.info("fs(%s %s) is already mounted"%
				(self.dev, self.mnt))
                        return 0
		if not os.path.exists(self.mnt):
			os.mkdir(self.mnt, 0755)
                cmd = ['mount', '-t', self.type, '-o', self.mnt_opt, self.dev, self.mnt]
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		if ret != 0:
			if self.optional:
				log.info("failed, but marked optional ... go on")
                        	return 0
                        log.error("failed")
                        return 1
                return 0

	def stop(self):
		log = logging.getLogger('UMOUNT')
                if self.is_up() is False:
                        log.info("fs(%s %s) is already umounted"%
				(self.dev, self.mnt))
                        return 0
                cmd = ['umount', self.mnt]
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		if ret != 0:
                        log.error("failed")
                        return 1
                return 0

