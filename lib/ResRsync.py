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
import logging

from rcGlobalEnv import *
from rcUtilities import process_call_argv, which
import rcStatus
import resources

def sync(self, section, log):
    if not self.svc.conf.has_option("default", section):
        log.info('no %s setup for synchronization' % section)
        return 0
    if section not in self.target:
        log.debug('%s => %s sync not applicable to %s', (self.src, self.dst, section))
        return 0
    for node in self.svc.conf.get("default", section).split(' '):
        if node == rcEnv.nodename:
            continue
        dst = node + ':' + self.dst
        cmd = self.cmd
        cmd.append(dst)
        log.info(' '.join(cmd))
        (ret, out) = process_call_argv(cmd)
        if ret != 0:
            log.error("node %s synchronization failed (%s => %s)" % (node, self.src, self.dst))
            return 1
    return 0

class Rsync(resources.Resource):
    timeout = 3600
    options = [ '-HpogDtrlvx', '--stats', '--delete', '--force' ]

    def syncnodes(self):
        log = logging.getLogger('SYNCNODES')
        return sync(self, "nodes", log)

    def syncdrp(self):
        log = logging.getLogger('SYNCDRP')
        return sync(self, "drpnode", log)

    def __init__(self, svc, src, dst, exclude='', target=['nodes', 'drpnode'],
                 optional=False, disabled=False):
        self.svc = svc
        self.src = src
        self.dst = dst
        self.exclude = exclude
        self.target = target
        self.options.append('--timeout=' + str(self.timeout))
        self.cmd = ['rsync'] + self.options + [self.exclude, self.src]
        resources.Resource.__init__(self,"rsync",optional,disabled)

    def __str__(self):
        return "%s src=%s dst=%s exclude=%s target=%s" % (Res.Resource.__str__(self),\
                self.src, self.dst, self.exclude, str(self.target))

