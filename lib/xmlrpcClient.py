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
import os
from rcGlobalEnv import rcEnv
hostId = __import__('hostid'+rcEnv.sysname)

hostid = hostId.hostid()
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

def end_action(svc, action, begin, end, logfile):
    err = 'ok'
    dateprev = None
    lines = open(logfile, 'r').read()

    """ If logfile is empty, default to current process pid
    """
    pid = os.getpid()

    """Example logfile line:
    2009-11-11 01:03:25,252;DISK.VG;INFO;unxtstsvc01_data is already up;10200;EOL
    """
    for line in lines.split(';EOL\n'):
        if line.count(';') != 4:
            continue
        date = line.split(';')[0]

        """Push to database the previous line, so that begin and end
        date are available.
        """
        if dateprev is not None:
            try:
                proxy.res_action(
                    ['svcname',
                     'action',
                     'hostname',
                     'hostid',
                     'pid',
                     'begin',
                     'end',
                     'status_log',
                     'status'],
                    [repr(svc.svcname),
                     repr(res.lower()+' '+action),
                     repr(rcEnv.nodename),
                     repr(hostid),
                     repr(pid),
                     repr(str(dateprev)),
                     repr(str(date)),
                     repr(str(msg)),
                     repr(str(res_err))]
                )
            except:
                pass

        res_err = 'ok'
        (date, res, lvl, msg, pid) = line.split(';')
        if lvl is None or lvl == 'DEBUG':
            continue
        if lvl == 'ERROR':
            err = 'err'
            res_err = 'err'
        if lvl == 'WARNING' and err != 'err':
            err = 'warn'
        if lvl == 'WARNING' and res_err != 'err':
            res_err = 'warn'
        dateprev = date

    """Push the last log entry, using 'end' as end date
    """
    try:
        proxy.res_action(
            ['svcname',
             'action',
             'hostname',
             'hostid',
             'pid',
             'begin',
             'end',
             'status_log',
             'status'],
            [repr(svc.svcname),
             repr(res.lower()+' '+action),
             repr(rcEnv.nodename),
             repr(hostid),
             repr(pid),
             repr(str(dateprev)),
             repr(str(end)),
             repr(str(msg)),
             repr(str(res_err))]
        )
    except:
        pass

    """Complete the wrap-up database entry
    """
    try:
        proxy.end_action(
            ['svcname',
             'action',
             'hostname',
             'hostid',
             'pid',
             'begin',
             'end',
             'time',
             'status'],
            [repr(svc.svcname),
             repr(action),
             repr(rcEnv.nodename),
             repr(hostid),
             repr(pid),
             repr(str(begin)),
             repr(str(end)),
             repr(str(end-begin)),
             repr(str(err))]
        )
    except:
        pass

def svcmon_update(svc, status):
    try:
        vars = [\
            "mon_svcname",
            "mon_svctype",
            "mon_nodname",
            "mon_nodtype",
            "mon_hostid",
            "mon_ipstatus",
            "mon_diskstatus",
            "mon_containerstatus",
            "mon_fsstatus",
            "mon_overallstatus",
            "mon_updated",
            "mon_prinodes"]
        vals = [\
            repr(svc.svcname),
            repr(svc.svctype),
            repr(rcEnv.nodename),
            repr(rcEnv.host_mode),
            repr(hostid),
            repr(str(status["ip"])),
            repr(str(status["disk"])),
            repr(str(status["container"])),
            repr(str(status["fs"])),
            repr(str(status["overall"])),
            repr(str(datetime.now())),
            repr(' '.join(svc.nodes))]
        proxy.svcmon_update(vars, vals)
    except:
        pass

