#!/usr/bin/python
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
from datetime import datetime
import xmlrpclib
import uuid
from rcGlobalEnv import rcEnv

hostid = str(uuid.getnode())
proxy = xmlrpclib.ServerProxy(rcEnv.dbopensvc)

def begin_action(svc, action, begin):
    try:
        proxy.begin_action(
            ['svcname',
             'action',
             'hostname',
             'hostid',
             'begin',],
            [repr(svc.svcname),
             repr(action),
             repr(rcEnv.nodename),
             repr(hostid),
             repr(str(begin))]
        )
    except:
        pass

def end_action(svc, action, begin, end, ret):
    try:
        proxy.end_action(
            ['svcname',
             'action',
             'hostname',
             'hostid',
             'begin',
             'end',
             'time',
             'status'],
            [repr(svc.svcname),
             repr(action),
             repr(rcEnv.nodename),
             repr(hostid),
             repr(str(begin)),
             repr(str(end)),
             repr(str(end-begin)),
             repr(str(ret))]
        )
    except:
        pass

def svcmon_update(svc, status):
    try:
        vars = [\
            "mon_svcname",
            "mon_svctype",
            "mon_nodname",
            "mon_ipstatus",
            "mon_fsstatus",
            "mon_prinodes"]
        vals = [\
            repr(svc.svcname),
            repr(svc.svctype),
            repr(rcEnv.nodename),
            repr(str(status["ip"])),
            repr(str(status["disk"]+status["mount"])),
            repr(' '.join(svc.nodes))]
        proxy.svcmon_update(vars, vals)
    except:
        pass

