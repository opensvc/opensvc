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
from subprocess import *
from datetime import datetime
import os
import glob
import logging

from rcGlobalEnv import rcEnv

def app(self, name, action):
	if action == 'start':
		log = logging.getLogger('STARTAPP')
	else:
		log = logging.getLogger('STOPAPP')

	log.info('spawn: %s %s' % (name, action))
	outf = '/var/tmp/svc_'+self.svc.svcname+'_'+os.path.basename(name)+'.log'
	f = open(outf, 'a')
	t = datetime.now()
	f.write(str(t))
	p = Popen([name, action], stdout=PIPE)
	f.write(p.communicate()[0])
	len = datetime.now() - t
	log.info('%s done in %s - ret %i - logs in %s' % (action, len, p.returncode, outf))
	f.close()
	return p.returncode

class Apps:
	def start(self):
		"""Execute each startup script (S* files). Log the return code but
		don't stop on error.
		"""
		for name in glob.glob(os.path.join(rcEnv.svcinitd, 'S*')):
			app(self, name, 'start')
		return 0

	def stop(self):
		"""Execute each shutdown script (K* files). Log the return code but
		don't stop on error.
		"""
		for name in glob.glob(os.path.join(rcEnv.svcinitd, 'K*')):
			app(self, name, 'stop')
		return 0

	def __init__(self, svc):
		self.svc = svc
