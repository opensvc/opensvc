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
from datetime import datetime
from subprocess import *

import rcLogger

def lxc(self, action):
	if action == 'start':
                log = logging.getLogger('STARTLXC')
		cmd = ['lxc-start', '-d', '-n', self.svcname]
        elif action == 'stop':
                log = logging.getLogger('STOPLXC')
		cmd = ['lxc-stop', '-d', '-n', self.svcname]
	else:
                log = logging.getLogger()
		log.error("unsupported lxc action: %s" % action)
		return 1

	log.info('spawn: %s' % ' '.join(cmd))
	outf = '/var/tmp/svc_'+self.svcname+'_lxc_'+action+'.log'
        f = open(outf, 'a')
        t = datetime.now()
        f.write(str(t))
        p = Popen(cmd, stdout=PIPE)
        ret = p.wait()
        f.write(p.communicate()[0])
        len = datetime.now() - t
        log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
        f.close()

class Lxc:
        def start(self):
		return lxc(self, 'start')

        def stop(self):
		return lxc(self, 'stop')

        def is_up(self):
		cmd = [ 'grep', '-w', self.svcname, '/proc/[0-9]*/cgroup' ]
		if os.spawnlp(os.P_WAIT, cmd) == 0:
			return True
		return False

	def __init__(self, svcname):
		self.svcname = svcname

