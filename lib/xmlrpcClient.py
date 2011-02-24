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
import rcStatus
import socket
import httplib

class TimeoutHTTP(httplib.HTTP):
   def __init__(self, host='', port=None, strict=None,
                timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
       if port == 0:
           port = None
       self._setup(self._connection_class(host, port, strict, timeout))

class TimeoutTransport(xmlrpclib.Transport):
    def __init__(self, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, *args, **kwargs):
        xmlrpclib.Transport.__init__(self, *args, **kwargs)
        self.timeout = timeout

    def make_connection(self, host):
        host, extra_headers, x509 = self.get_host_info(host)
        conn = TimeoutHTTP(host, timeout=self.timeout)
        return conn

class TimeoutServerProxy(xmlrpclib.ServerProxy):
    def __init__(self, uri, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                 *args, **kwargs):
        kwargs['transport'] = TimeoutTransport(timeout=timeout,
                                               use_datetime=kwargs.get('use_datetime', 0))
        xmlrpclib.ServerProxy.__init__(self, uri, *args, **kwargs)

sysname, nodename, x, x, machine = os.uname()
hostId = __import__('hostid'+sysname)
hostid = hostId.hostid()

def xmlrpc_decorator_dummy(fn):
    def new(*args):
        pass
    return new

def xmlrpc_decorator(fn):
    def new(*args):
        try:
            return fn(*args)
        except (socket.error, xmlrpclib.ProtocolError):
            """ normal for collector communications disabled
                through 127.0.0.1 == dbopensvc
            """
            pass
        except socket.timeout:
            print "connection to collector timed out"
        except:
            import sys
            import traceback
            e = sys.exc_info()
            print e[0], e[1], traceback.print_tb(e[2])
    return new

try:
    a = socket.getaddrinfo(rcEnv.dbopensvc_host, None)
    if len(a) == 0:
        raise Exception
except:
    print("could not resolve %s to an ip address. disable collector updates."%rcEnv.dbopensvc)
    xmlrpc_decorator = xmlrpc_decorator_dummy

proxy = TimeoutServerProxy(rcEnv.dbopensvc, timeout=20)
try:
    proxy_methods = proxy.system.listMethods()
except:
    proxy_methods = []

comp_proxy = TimeoutServerProxy(rcEnv.dbcompliance, timeout=20)
try:
    comp_proxy_methods = comp_proxy.system.listMethods()
except:
    comp_proxy_methods = []

@xmlrpc_decorator
def begin_action(svc, action, begin):
    try:
        import version
        version = version.version
    except:
        version = "0";

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

@xmlrpc_decorator
def end_action(svc, action, begin, end, logfile):
    err = 'ok'
    dateprev = None
    lines = open(logfile, 'r').read()
    pids = set([])

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

        # database overflow protection
        trim_lim = 10000
        trim_tag = ' <trimmed> '
        trim_head = int(trim_lim/2)
        trim_tail = trim_head-len(trim_tag)
        if len(msg) > trim_lim:
            msg = msg[:trim_head]+' <trimmed> '+msg[-trim_tail:]

        pids |= set([pid])
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
        proxy.res_action_batch(vars, vals)

    """Complete the wrap-up database entry
    """

    """ If logfile is empty, default to current process pid
    """
    if len(pids) == 0:
        pids = set([os.getpid()])

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
         repr(','.join(map(str, pids))),
         repr(str(begin)),
         repr(str(end)),
         repr(str(end-begin)),
         repr(str(err))]
    )

@xmlrpc_decorator
def svcmon_update_combo(g_vars, g_vals, r_vars, r_vals):
    if 'svcmon_update_combo' in proxy_methods:
        proxy.svcmon_update_combo(g_vars, g_vals, r_vars, r_vals)
    else:
        proxy.svcmon_update(g_vars, g_vals)
        proxy.resmon_update(r_vars, r_vals)

@xmlrpc_decorator
def push_service(svc):
    def envfile(svc):
        envfile = os.path.join(rcEnv.pathsvc, 'etc', svc+'.env')
        if not os.path.exists(envfile):
            return
        with open(envfile, 'r') as f:
            buff = ""
            for line in f.readlines():
                l = line.strip()
                if len(l) == 0:
                    continue
                if l[0] == '#' or l[0] == ';':
                    continue
                buff += line
            return buff
        return

    try:
        import version
        version = version.version
    except:
        version = "0";

    if hasattr(svc, "guestos"):
        guestos = svc.guestos
    else:
        guestos = ""

    vars = ['svc_hostid',
            'svc_name',
            'svc_vmname',
            'svc_cluster_type',
            'svc_flex_min_nodes',
            'svc_flex_max_nodes',
            'svc_flex_cpu_low_threshold',
            'svc_flex_cpu_high_threshold',
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
            'svc_drnoaction',
            'svc_guestos']

    vals = [repr(hostid),
            repr(svc.svcname),
            repr(svc.vmname),
            repr(svc.clustertype),
            repr(svc.flex_min_nodes),
            repr(svc.flex_max_nodes),
            repr(svc.flex_cpu_low_threshold),
            repr(svc.flex_cpu_high_threshold),
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
            repr(svc.drnoaction),
            repr(guestos)]

    if 'container' in svc.resources_by_id:
        container_info = svc.resources_by_id['container'].get_container_info()
        vars += ['svc_vcpus', 'svc_vmem']
        vals += [container_info['vcpus'],
                 container_info['vmem']]

    proxy.update_service(vars, vals)

@xmlrpc_decorator
def delete_services():
    proxy.delete_services(hostid)

@xmlrpc_decorator
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

@xmlrpc_decorator
def push_stats_fs_u(l):
    proxy.insert_stats_fs_u(l[0], l[1])

@xmlrpc_decorator
def push_pkg():
    p = __import__('rcPkg'+sysname)
    vars = ['pkg_nodename',
            'pkg_name',
            'pkg_version',
            'pkg_arch']
    vals = p.listpkg()
    proxy.delete_pkg(rcEnv.nodename)
    proxy.insert_pkg(vars, vals)

@xmlrpc_decorator
def push_patch():
    p = __import__('rcPkg'+sysname)
    vars = ['patch_nodename',
            'patch_num',
            'patch_rev']
    vals = p.listpatch()
    proxy.delete_patch(rcEnv.nodename)
    proxy.insert_patch(vars, vals)

def push_stats(force=False, file=None, collect_date=None, interval=15):
    try:
        s = __import__('rcStats'+sysname)
    except ImportError:
        return
    sp = s.StatsProvider(collect_file=file,
                         collect_date=collect_date,
                         interval=interval)
    h = {}
    for stat in ['cpu', 'mem_u', 'proc', 'swap', 'block',
                 'blockdev', 'netdev', 'netdev_err']:
        h[stat] = sp.get(stat)
    import cPickle
    proxy.insert_stats(cPickle.dumps(h))

def push_asset(node):
    try:
        m = __import__('rcAsset'+sysname)
    except ImportError:
        print "pushasset methods not implemented on", sysname
        return
    if "update_asset" not in proxy_methods:
        print "'update_asset' method is not exported by the collector"
        return
    d = m.Asset(node).get_asset_dict()
    proxy.update_asset(d.keys(), d.values())

def push_sym():
    if 'update_sym_xml' not in proxy_methods:
	print "'update_sym_xml' method is not exported by the collector"
	return
    m = __import__('rcSymmetrix')
    try:
        syms = m.Syms()
    except:
        return
    for sym in syms:
        vals = []
        for key in sym.keys:
            vals.append(getattr(sym, 'get_'+key)())
        proxy = TimeoutServerProxy(rcEnv.dbopensvc, timeout=180)
        proxy.update_sym_xml(sym.sid, sym.keys, vals)

@xmlrpc_decorator
def push_all(svcs):
    proxy.delete_service_list([svc.svcname for svc in svcs])
    for svc in svcs:
        push_disks(svc)
        push_service(svc)

@xmlrpc_decorator
def push_checks(vars, vals):
    if "push_checks" not in proxy_methods:
        print "'push_checks' method is not exported by the collector"
        return
    proxy.push_checks(vars, vals)

@xmlrpc_decorator
def comp_get_moduleset_modules(moduleset):
    return comp_proxy.comp_get_moduleset_modules(moduleset)

@xmlrpc_decorator
def comp_get_moduleset():
    return comp_proxy.comp_get_moduleset(rcEnv.nodename)

@xmlrpc_decorator
def comp_attach_moduleset(moduleset):
    return comp_proxy.comp_attach_moduleset(rcEnv.nodename, moduleset)

@xmlrpc_decorator
def comp_detach_moduleset(moduleset):
    return comp_proxy.comp_detach_moduleset(rcEnv.nodename, moduleset)

@xmlrpc_decorator
def comp_get_ruleset():
    return comp_proxy.comp_get_ruleset(rcEnv.nodename)

@xmlrpc_decorator
def comp_get_dated_ruleset(date):
    return comp_proxy.comp_get_dated_ruleset(rcEnv.nodename, date)

@xmlrpc_decorator
def comp_attach_ruleset(ruleset):
    return comp_proxy.comp_attach_ruleset(rcEnv.nodename, ruleset)

@xmlrpc_decorator
def comp_detach_ruleset(ruleset):
    return comp_proxy.comp_detach_ruleset(rcEnv.nodename, ruleset)

@xmlrpc_decorator
def comp_list_ruleset(pattern='%'):
    return comp_proxy.comp_list_rulesets(pattern)

@xmlrpc_decorator
def comp_list_moduleset(pattern='%'):
    return comp_proxy.comp_list_modulesets(pattern)

@xmlrpc_decorator
def comp_log_action(vars, vals):
    return comp_proxy.comp_log_action(vars, vals)
