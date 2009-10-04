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
import logging.handlers
from rcGlobalEnv import *

class Logger(logging.Logger):
	def __init__(self, name):
		logging.Logger.__init__(self, name)
		formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
		filehandler = logging.handlers.RotatingFileHandler(rcEnv.logfile, maxBytes=5000, backupCount=5)
		streamhandler = logging.StreamHandler()
		filehandler.setFormatter(formatter)
		streamhandler.setFormatter(formatter)
		self.addHandler(filehandler)
		self.addHandler(streamhandler)
		try:
			self.setLevel(rcEnv.loglevel)
		except:
			pass
