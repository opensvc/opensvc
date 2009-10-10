import os
import re
import logging

from rcGlobalEnv import *
from rcUtilities import process_call_argv, which
import rcStatus

def file_to_loop(f):
	"""Given a file path, returns the loop device associated. For example,
	/path/to/file => /dev/loop0
	"""
	if which('losetup') is None:
		return None
	if not os.path.isfile(f):
		return None
	if rcEnv.sysname != 'Linux':
		return None
	(ret, out) = process_call_argv(['losetup', '-j', f])
	if len(out) == 0:
		return None
	return out.split()[0].strip(':')

class Loop:
	def is_up(self):
		"""Returns True if the volume group is present and activated
		"""
		self.loop = file_to_loop(self.file)
		if self.loop is None:
			return False
		return True

	def start(self):
		log = logging.getLogger('STARTLOOP')
		if self.is_up():
			log.info("%s is already up" % self.file)
			return 0
		cmd = [ 'losetup', '-f', self.file ]
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		if ret == 0:
			self.loop = file_to_loop(self.file)
		log.info("%s now loops to %s" % (self.loop, self.file))
		return ret

	def stop(self):
		log = logging.getLogger('STOPLOOP')
		if not self.is_up():
			log.info("%s is already down" % self.file)
			return 0
		cmd = [ 'losetup', '-d', self.loop ]
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		return ret

	def status(self, verbose=False):
		if self.is_up():
			return rcStatus.UP
		else:
			return rcStatus.DOWN

	def __init__(self, file):
		self.file = file
