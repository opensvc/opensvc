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
import re
import logging

from rcUtilities import process_call_argv
import rcStatus
import vg

class Vg(vg.Vg):
	def has_vg(self):
		"""Returns True if the volume is present
		"""
		cmd = [ 'vgs', '--noheadings', '-o', 'name' ]
		(ret, out) = process_call_argv(cmd)
		if re.match('\s*'+self.name+'\s', out, re.MULTILINE) is None:
			return False
		return True

	def is_up(self):
		"""Returns True if the volume group is present and activated
		"""
		if not self.has_vg():
			return False
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
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		return ret

	def stop(self):
		log = logging.getLogger('STOPVG')
		if not self.is_up():
			log.info("%s is already down" % self.name)
			return 0
		cmd = [ 'vgchange', '-a', 'n', self.name ]
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		return ret

	def status(self, verbose=False):
		if self.is_up():
			status = rcStatus.UP
		else:
			status = rcStatus.DOWN
		if (verbose):
			rcStatus.print_status("vg %s" % self.name, status)
		return status

	def __init__(self, name):
		self.name = name
                vg.Vg.__init__(self)
