#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@free.fr>'
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

def hostid():
    cmd = ['uname', '-u']
    p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
    buff = p.communicate()
    if p.returncode != 0:
        return '1'
    sn = buff[0].split()[0]
    return str(hex(abs(sn.__hash__()))).replace('0x', '')
