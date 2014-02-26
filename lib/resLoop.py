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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import resources as Res
from rcGlobalEnv import rcEnv

class Loop(Res.Resource):
    """ basic loopback device resource
    """
    def __init__(self,
                 rid=None,
                 loopFile=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        Res.Resource.__init__(self,
                              rid,
                              "disk.loop",
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              monitor=monitor,
                              restart=restart,
                              subset=subset)
        self.loopFile = loopFile
        self.label = loopFile
        self.always_on = always_on

    def __str__(self):
        return "%s loopfile=%s" % (Res.Resource.__str__(self),\
                                 self.loopFile)

if __name__ == "__main__":
    for c in (Loop,) :
        help(c)

    print("""v1=vg("myvg")""")
    v=vg("myvg")
    print("show v", v)
    print("""v.do_action("start")""")
    v.do_action("start")

