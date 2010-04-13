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
from datetime import datetime, timedelta
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

def push_stats_cpu():
    try:
        s = __import__('rcStats'+sysname)
    except:
        return
    proxy.insert_stats_cpu(
        ['date',
         'cpu',
         'usr',
         'nice',
         'sys',
         'iowait',
         'steal',
         'irq',
         'soft',
         'guest',
         'idle',
         'nodename'],
         s.stats_cpu()
    )

def push_stats_mem_u():
    try:
        s = __import__('rcStats'+sysname)
    except:
        return
    proxy.insert_stats_mem_u(
        ['date',
         'kbmemfree',
         'kbmemused',
         'pct_memused',
         'kbbuffers',
         'kbcached',
         'kbcommit',
         'pct_commit',
         'nodename'],
         s.stats_mem_u()
    )

def push_stats_proc():
    try:
        s = __import__('rcStats'+sysname)
    except:
        return
    proxy.insert_stats_proc(
        ['date',
         'runq_sz',
         'plist_sz',
         'ldavg_1',
         'ldavg_5',
         'ldavg_15',
         'nodename'],
         s.stats_proc()
    )

def push_stats_swap():
    try:
        s = __import__('rcStats'+sysname)
    except:
        return
    proxy.insert_stats_swap(
        ['date',
         'kbswpfree',
         'kbswpused',
         'pct_swpused',
         'kbswpcad',
         'pct_swpcad',
         'nodename'],
         s.stats_swap()
    )

def push_stats_block():
    try:
        s = __import__('rcStats'+sysname)
    except:
        return
    proxy.insert_stats_block(
        ['date',
         'tps',
         'rtps',
         'wtps',
         'rbps',
         'wbps',
         'nodename'],
         s.stats_block()
    )

def push_stats_blockdev():
    try:
        s = __import__('rcStats'+sysname)
    except:
        return
    proxy.insert_stats_blockdev(
        ['date',
         'dev',
         'tps',
         'rsecps',
         'wsecps',
         'avgrq_sz',
         'avgqu_sz',
         'await',
         'svctm',
         'pct_util',
         'nodename'],
         s.stats_blockdev()
    )

def check_stats_timestamp(sync_timestamp_f, comp='more', delay=10):
    if not os.path.exists(sync_timestamp_f):
        return True
    try:
        with open(sync_timestamp_f, 'r') as f:
            d = f.read()
            last = datetime.strptime(d,"%Y-%m-%d %H:%M:%S.%f\n")
            limit = last + timedelta(minutes=delay)
            if comp == "more" and datetime.now() < limit:
                return False
            elif comp == "less" and datetime.now() < limit:
                return False
            else:
                return True
            f.close()
    except:
        return True
    return True

def stats_timestamp():
    sync_timestamp_f = os.path.join(rcEnv.pathvar, 'last_stats_push')
    if not check_stats_timestamp(sync_timestamp_f, 'more', 10):
        return False
    sync_timestamp_d = os.path.dirname(sync_timestamp_f)
    if not os.path.isdir(sync_timestamp_d):
        os.makedirs(sync_timestamp_d ,0755)
    with open(sync_timestamp_f, 'w') as f:
        f.write(str(datetime.now())+'\n')
        f.close()
    return True

def push_stats():
    if not stats_timestamp():
        return
    push_stats_cpu()
    push_stats_mem_u()
    push_stats_proc()
    push_stats_swap()
    push_stats_block()
    push_stats_blockdev()

def push_all(svcs):
    proxy.delete_service_list([svc.svcname for svc in svcs])
    push_stats()
    for svc in svcs:
        push_disks(svc)
        push_service(svc)
