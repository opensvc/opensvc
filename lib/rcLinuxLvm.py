import re
import logging

from rcUtilities import process_call_argv

class Vg:
	def has_vg(self):
		print "TODO: rcVg.has_vg"
		return 0

	def is_up(self):
		cmd = [ 'lvs', '--noheadings', '-o', 'lv_attr', self.name ]
		(ret, out) = process_call_argv(cmd)
		if re.match(' ....-[-o]', out, re.MULTILINE) is None:
			return True
		return False

	def start(self):
		log = logging.getLogger('STARTVG')
		if self.is_up():
			log.info("%s is already up" % self.name)
			return 0
		cmd = [ 'vgchange', '-a', 'y', self.name ]
		(ret, out) = process_call_argv(cmd)
		return ret

	def stop(self):
		log = logging.getLogger('STOPVG')
		if not self.is_up():
			log.info("%s is already down" % self.name)
			return 0
		cmd = [ 'vgchange', '-a', 'n', self.name ]
		(ret, out) = process_call_argv(cmd)
		return ret

	def status(self):
		if self.is_up():
			return rcStatus.UP
		else:
			return rcStatus.DOWN

	def __init__(self, name):
		self.name = name
