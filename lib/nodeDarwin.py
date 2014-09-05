#
# Copyright (c) 2014 Christophe Varoqui <christophe.varoqui@opensvc.com>
# Copyright (c) 2014 Cyril Galibern <cyril.galibern@opensvc.com>
# Copyright (c) 2014 Arnaud Veron <arnaud.veron@opensvc.com>
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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import node

class Node(node.Node):
    def shutdown(self):
        cmd = ["shutdown", "-h", "now"]
        ret, out, err = self.vcall(cmd)

    def reboot(self):
        cmd = ["shutdown", "-r", "now"]
        ret, out, err = self.vcall(cmd)
