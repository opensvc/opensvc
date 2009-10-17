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
import re
import logging

from rcGlobalEnv import *
from rcUtilities import process_call_argv, which
import rcStatus
import loop

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

class Loop(loop.Loop):
    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        self.loop = file_to_loop(self.loopFile)
        if self.loop is None:
            return False
        return True

    def start(self):
        log = logging.getLogger('STARTLOOP')
        if self.is_up():
            log.info("%s is already up" % self.loopFile)
            return 0
        cmd = [ 'losetup', '-f', self.loopFile ]
        log.info(' '.join(cmd))
        (ret, out) = process_call_argv(cmd)
        if ret == 0:
            self.loop = file_to_loop(self.loopFile)
        log.info("%s now loops to %s" % (self.loop, self.loopFile))
        return ret

    def stop(self):
        log = logging.getLogger('STOPLOOP')
        if not self.is_up():
            log.info("%s is already down" % self.loopFile)
            return 0
        cmd = [ 'losetup', '-d', self.loop ]
        log.info(' '.join(cmd))
        (ret, out) = process_call_argv(cmd)
        return ret

    def status(self):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

    def __init__(self, loopFile):
        loop.Loop.__init__(self, loopFile)
