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

from rcGlobalEnv import *
import rcIp
import rcHosted

def lxc(action):
	if action == 'start':
                log = logging.getLogger('STARTLXC')
        else:
                log = logging.getLogger('STOPLXC')

	log.info('spawn: lxc-%s -b -n %s' % (action, rcEnv.svcname))
	outf = '/var/tmp/svc_'+rcEnv.svcname+'_lxc_'+action+'.log'
	cmd = ['lxc-start -b -n ', rcEnv.svcname]
        f = open(outf, 'a')
        t = datetime.now()
        f.write(str(t))
        p = Popen(cmd, stdout=PIPE)
        ret = p.wait()
        f.write(p.communicate()[0])
        len = datetime.now() - t
        log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
        f.close()

def start():
	startip()
	mount()
	startlxc()
	startapp()

def stop():
	stopapp()
	stoplxc()
	umount()
	stopip()

def startip():
	"""startip is a noop for LXC : ips are plumbed on container start-up
	"""
	log = logging.getLogger('STARTIP')
	log.info('no-op')

def stopip():
	"""stopip is a noop for LXC : ips are unplumbed on container stop
	"""
	log = logging.getLogger('STOPIP')
	log.info('no-op')

def mount():
	log = logging.getLogger('MOUNT')
	if rcHosted.mount() != 0:
		return 1
	return 0

def umount():
	log = logging.getLogger('UMOUNT')
	if rcHosted.umount() != 0:
		return 1
	return 0

def startlxc():
	log = logging.getLogger('STARTLXC')
	if lxc(start) != 0:
		return 1
	return 0

def stoplxc():
	log = logging.getLogger('STOPLXC')
	if lxc(stop) != 0:
		return 1
	return 0

def startapp():
	log = logging.getLogger('STARTAPP')
	return 0

def stopapp():
	log = logging.getLogger('STOPAPP')
	return 0

def configure():
	log = logging.getLogger('CONFIGURE')
	return 0

def syncnodes():
	log = logging.getLogger('SYNCNODES')
	return 0

def syncdrp():
	log = logging.getLogger('SYNCDRP')
	return 0

def create(self):
	log = logging.getLogger('CREATE')
	return 0
