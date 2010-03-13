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

sysname, nodename, x, x, machine = os.uname()
hostId = __import__('hostid'+sysname)
hostid = hostId.hostid()
proxy = xmlrpclib.ServerProxy(rcEnv.dbopensvc)

def begin_action(svc, action, begin):
    try:
        import version
        version = version.version
    except:
        version = "0";

    try:
        proxy.begin_action(
            ['svcname',
             'action',
             'hostname',
             'hostid',
             'version',
             'begin',],
            [repr(svc.svcname),
             repr(action),
             repr(rcEnv.nodename),
             repr(hostid),
             repr(version),
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
    vars = ['svcname',
            'action',
            'hostname',
            'hostid',
            'pid',
            'begin',
            'end',
            'status_log',
            'status']
    vals = []
    for line in lines.split(';EOL\n'):
        if line.count(';') != 4:
            continue
        date = line.split(';')[0]

        """Push to database the previous line, so that begin and end
        date are available.
        """
        if dateprev is not None:
            res = res.lower()
            res = res.replace(svc.svcname+'.','')
            vals.append([svc.svcname,
                         res+' '+action,
                         rcEnv.nodename,
                         hostid,
                         pid,
                         dateprev,
                         date,
                         msg,
                         res_err])

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
    if dateprev is not None:
        res = res.lower()
        res = res.replace(svc.svcname+'.','')
        vals.append([svc.svcname,
                     res+' '+action,
                     rcEnv.nodename,
                     hostid,
                     pid,
                     dateprev,
                     date,
                     msg,
                     res_err])

    if len(vals) > 0:
        try:
            proxy.res_action_batch(vars, vals)
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
            "mon_syncstatus",
            "mon_containerstatus",
            "mon_fsstatus",
            "mon_appstatus",
            "mon_overallstatus",
            "mon_updated",
            "mon_prinodes"]
        vals = [\
            svc.svcname,
            svc.svctype,
            rcEnv.nodename,
            rcEnv.host_mode,
            hostid,
            str(status["ip"]),
            str(status["disk"]),
            str(status["sync"]),
            str(status["container"]),
            str(status["fs"]),
            str(status["app"]),
            str(status["overall"]),
            str(datetime.now()),
            ' '.join(svc.nodes)]
        proxy.svcmon_update(vars, vals)
    except:
        raise
        pass
    resmon_update(svc, status)

def resmon_update(svc, status):
    vals = []
    now = datetime.now()
    for rs in svc.resSets:
        for r in rs.resources:
            vals.append([repr(svc.svcname),
                         repr(rcEnv.nodename),
                         repr(r.rid),
                         repr(r.label),
                         repr(str(r.rstatus)),
                         repr(str(now))]
            )
    vars = [\
        "svcname",
        "nodename",
        "rid",
        "res_desc",
        "res_status",
        "updated"]
    proxy.resmon_update(vars, vals)
    try:
        proxy.resmon_update(vars, vals)
    except:
        pass

def push_ips(svc):
    proxy.delete_ips(svc.svcname, rcEnv.nodename)
    vars = ['ip_svcname',
            'ip_dev',
            'ip_name',
            'ip_node',
            'ip_netmask']
    vals = []
    for rset in svc.get_res_sets("ip"):
        for r in rset.resources:
            vals.append(
                [svc.svcname,
                 r.ipDev,
                 r.ipName,
                 rcEnv.nodename,
                 str(r.mask)]
            )
    proxy.register_ip(vars, vals)

def push_fss(svc):
    proxy.delete_fss(svc.svcname)
    vars = ['fs_svcname',
            'fs_dev',
            'fs_mnt',
            'fs_mntopt',
            'fs_type']
    vals = []

    for rset in svc.get_res_sets("fs"):
        for r in rset.resources:
            vals.append(
                [svc.svcname,
                 r.device,
                 r.mountPoint,
                 r.mntOpt,
                 r.fsType]
            )
    proxy.register_fs(vars, vals)

def push_rsyncs(svc):
    pass

def push_service(svc):
    def envfile(svc):
        envfile = os.path.join(rcEnv.pathsvc, 'etc', svc+'.env')
        if not os.path.exists(envfile):
            return
        with open(envfile, 'r') as f:
            buff = f.read()
            return buff
        return

    try:
        import version
        version = version.version
    except:
        version = "0";

    proxy.update_service(
        ['svc_hostid',
         'svc_name',
         'svc_vmname',
         'svc_type',
         'svc_nodes',
         'svc_drpnode',
         'svc_drpnodes',
         'svc_comment',
         'svc_drptype',
         'svc_autostart',
         'svc_app',
         'svc_containertype',
         'svc_envfile',
         'svc_version',
         'svc_drnoaction'],
        [repr(hostid),
         repr(svc.svcname),
         repr(svc.vmname),
         repr(svc.svctype),
         repr(' '.join(svc.nodes)),
         repr(svc.drpnode),
         repr(' '.join(svc.drpnodes)),
         repr(svc.comment),
         repr(svc.drp_type),
         repr(svc.autostart_node),
         repr(svc.app),
         repr(svc.svcmode),
         repr(envfile(svc.svcname)),
         repr(version),
         repr(svc.drnoaction)]
    )

def delete_services():
    proxy.delete_services(hostid)

def push_disks(svc):
    def disk_dg(dev, svc):
        for rset in svc.get_res_sets("disk.vg"):
            for vg in rset.resources:
                if vg.is_disabled():
                    continue
                if not vg.name in disklist_cache:
                    disklist_cache[vg.name] = vg.disklist()
                if dev in disklist_cache[vg.name]:
                    return vg.name
        return ""

    di = __import__('rcDiskInfo'+sysname)
    disks = di.diskInfo()
    disklist_cache = {}

    proxy.delete_disks(svc.svcname, rcEnv.nodename)

    for d in svc.disklist():
        if disks.disk_id(d) is None or disks.disk_id(d) == "":
            """ no point pushing to db an empty entry
            """
            continue
        proxy.register_disk(
            ['disk_id',
             'disk_svcname',
             'disk_size',
             'disk_vendor',
             'disk_model',
             'disk_dg',
             'disk_nodename'],
            [repr(disks.disk_id(d)),
             repr(svc.svcname),
             repr(disks.disk_size(d)),
             repr(disks.disk_vendor(d)),
             repr(disks.disk_model(d)),
             repr(disk_dg(d, svc)),
             repr(rcEnv.nodename)]
        )

def push_all(svcs):
    proxy.delete_service_list([svc.svcname for svc in svcs])
    for svc in svcs:
        push_rsyncs(svc)
        push_ips(svc)
        push_fss(svc)
        push_disks(svc)
        push_service(svc)

