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

import rcExceptions as ex
from subprocess import *

rcIfconfig = __import__("rcIfconfigHP-UX")
from rcGlobalEnv import rcEnv

class ifconfig(rcIfconfig.ifconfig):
    def __init__(self, hostname):
        self.hwaddr = {}
        self.intf = []
        cmd = rcEnv.rsh.split(' ') + [hostname, 'netstat', '-win']
        p = Popen(cmd, stdout=PIPE)
        buff = p.communicate()[0]
        if p.returncode != 0:
            raise ex.excError
        intf_list = buff
        for line in intf_list.split('\n'):
            if len(line) == 0:
                continue
            if line.startswith("Name"):
                continue
            intf = line.split()[0].replace('*', '')
            cmd = rcEnv.rsh.split(' ') + [hostname, 'env', 'LANG=C', 'ifconfig', intf]
            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            out = p.communicate()
            if p.returncode != 0:
                continue
            if "no such interface" in out[1]:
                continue
            self.parse(out[0])
