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
import os

UP = 0
DOWN = 1
WARN = 2
NA = 3
TODO = 4
UNDEF = 5

GREEN = 32
RED = 31
YELLOW = 33

def _merge(s1, s2):
	"""Merge too status: WARN and TODO taint UP and DOWN
	"""
	if s1 == UNDEF: return s2
	if (s1, s2) == (UP, UP): return UP
	if (s1, s2) == (UP, DOWN): return WARN
	if (s1, s2) == (UP, WARN): return WARN
	if (s1, s2) == (UP, NA): return UP
	if (s1, s2) == (UP, TODO): return WARN
	if (s1, s2) == (DOWN, DOWN): return DOWN
	if (s1, s2) == (DOWN, WARN): return WARN
	if (s1, s2) == (DOWN, NA): return DOWN
	if (s1, s2) == (DOWN, TODO): return WARN
	if (s1, s2) == (WARN, WARN): return WARN
	if (s1, s2) == (WARN, NA): return WARN
	if (s1, s2) == (WARN, TODO): return WARN
	if (s1, s2) == (NA, NA): return NA
	if (s1, s2) == (NA, TODO): return WARN
	if (s1, s2) == (TODO, TODO): return TODO
	return _merge(s2, s1)

def colorize(color, text):
	if os.isatty(1):
		return '\033['+str(color)+'m'+text+'\033[m'
	else:
		return text

def status_str(s):
	if s == UP: return colorize(GREEN, 'UP')
	if s == DOWN: return colorize(RED, 'DOWN')
	if s == WARN: return colorize(YELLOW, 'WARN')
	if s == NA: return 'N/A'
	if s == TODO: return 'TODO'
	if s == UNDEF: return 'UNDEF'

def print_status(resource, status):
	import string
	print '{0:70} {1}'.format(resource, status_str(status))

class Status:
	"""Class that wraps printing and calculation of resource status
	"""
	status = UNDEF

	def reset(self):
		self.status = UNDEF

	def add(self, s):
		"""Merge a status with current global status
		"""
		self.status = _merge(self.status, s)
