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
import sys
from rcGlobalEnv import rcEnv
import rcStatus
import socket
import httplib
import rcExceptions as ex

hostId = __import__('hostid'+rcEnv.sysname)
hostid = hostId.hostid()
rcEnv.warned = False
pathosvc = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))

import logging
import logging.handlers
logfile = os.path.join(pathosvc, 'log', 'xmlrpc.log')
fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
filehandler = logging.handlers.RotatingFileHandler(os.path.join(logfile),
                                                   maxBytes=5242880,
                                                   backupCount=5)
filehandler.setFormatter(fileformatter)
log = logging.getLogger("xmlrpc")
log.addHandler(filehandler)
log.setLevel(logging.DEBUG)
log.debug("logger setup")

from multiprocessing import Queue, Process
from Queue import Empty

def call_worker(q):
    e = "foo"
    o = Collector(worker=True)
    o.init()
    try:
        while e is not None:
            e = q.get()
            if e is None:
                break
            fn, args, kwargs = e
            o.log.debug("xmlrpc async %s"%fn)
            try:
                getattr(o.proxy, fn)(*args, **kwargs)
                o.log.debug("xmlrpc async %s done"%fn)
                continue
            except (socket.error, xmlrpclib.ProtocolError):
                """ normal for collector communications disabled
                    through 127.0.0.1 == dbopensvc
                """
                pass
            except socket.timeout:
                o.log.error("connection to collector timed out")
            except:
                import traceback
                e = sys.exc_info()
                o.log.error(str((e[0], e[1], traceback.print_tb(e[2]))))
            o.log.error("xmlrpc async %s error"%fn)
        o.log.info("shutdown")
    except ex.excSignal:
        o.log.info("interrupted on signal")
 
class Collector(object):
    def split_url(self, url):
        if url == 'None':
            return 'https', '127.0.0.1', '443', '/'
        if url.startswith('https'):
            transport = 'https'
            url = url.replace('https://', '')
        elif url.startswith('http'):
            transport = 'http'
            url = url.replace('http://', '')
        l = url.split('/')
        if len(l) < 2:
            raise
        app = l[1]
        l = l[0].split(':')
        if len(l) == 1:
            host = l[0]
            if transport == 'http':
                port = '80'
            else:
                port = '443'
        elif len(l) == 2:
            host = l[0]
            port = l[1]
        else:
            raise
        return transport, host, port, app
    
    def setNodeEnv(self):
        import ConfigParser
        pathetc = os.path.join(os.path.dirname(__file__), '..', 'etc')
        nodeconf = os.path.join(pathetc, 'node.conf')
        config = ConfigParser.RawConfigParser()
        config.read(nodeconf)
        if config.has_option('node', 'dbopensvc'):
            rcEnv.dbopensvc = config.get('node', 'dbopensvc')
            try:
                rcEnv.dbopensvc_transport, rcEnv.dbopensvc_host, rcEnv.dbopensvc_port, rcEnv.dbopensvc_app = self.split_url(rcEnv.dbopensvc)
            except:
                self.log.error("malformed dbopensvc url: %s"%rcEnv.dbopensvc)
        if config.has_option('node', 'dbcompliance'):
            rcEnv.dbcompliance = config.get('node', 'dbcompliance')
            try:
                rcEnv.dbcompliance_transport, rcEnv.dbcompliance_host, rcEnv.dbcompliance_port, rcEnv.dbcompliance_app = self.split_url(rcEnv.dbcompliance)
            except:
                self.log.error("malformed dbcompliance url: %s"%rcEnv.dbcompliance)
        if config.has_option('node', 'uuid'):
            rcEnv.uuid = config.get('node', 'uuid')
        else:
            rcEnv.uuid = ""
        del(config)

    def submit(self, fn, *args, **kwargs):
        self.init_worker()
        self.log.debug("enqueue %s"%fn)
        self.queue.put((fn, args, kwargs), block=True)

    def call(self, *args, **kwargs):
        fn = args[0]
        self.init(fn)
        if len(self.proxy_methods) == 0:
            return
        self.log.debug("call %s"%fn)
        if len(args) > 1:
            args = args[1:]
        else:
            args = []
        if fn == "register_node" and \
           'register_node' not in self.proxy_methods:
            print >>sys.stderr, "collector does not support node registration"
            return
        if rcEnv.uuid == "" and \
           rcEnv.dbopensvc is not None and \
           not rcEnv.warned and \
           self.auth_node and \
           fn != "register_node":
            print >>sys.stderr, "this node is not registered. try 'nodemgr register'"
            print >>sys.stderr, "to disable this warning, set 'dbopensvc = None' in node.conf"
            rcEnv.warned = True
            return
        try:
            buff = getattr(self, fn)(*args, **kwargs)
            self.log.debug("call %s done"%fn)
            return buff
        except (socket.error, xmlrpclib.ProtocolError):
            """ normal for collector communications disabled
                through 127.0.0.1 == dbopensvc
            """
            pass
        except socket.timeout:
            print "connection to collector timed out"
        except:
            import traceback
            e = sys.exc_info()
            print e[0], e[1], traceback.print_tb(e[2])
        self.log.error("call %s error"%fn)
    
    def __init__(self, worker=False):
        self.proxy = None
        self.proxy_method = None
        self.comp_proxy = None
        self.comp_proxy_method = None

        self._worker = worker
        self.worker = None
        self.queue = None
        self.comp_fns = ['comp_get_moduleset_modules',
                         'comp_get_moduleset',
                         'comp_attach_moduleset',
                         'comp_detach_moduleset',
                         'comp_get_ruleset',
                         'comp_get_dated_ruleset',
                         'comp_attach_ruleset',
                         'comp_detach_ruleset',
                         'comp_list_ruleset',
                         'comp_list_moduleset',
                         'comp_log_action']
        self.method_cache = os.path.join(pathosvc, "var", "collector")
        self.auth_node = True
        self.log = logging.getLogger("xmlrpc%s"%('.worker' if worker else ''))

    def load_method_cache(self):
        if not os.path.exists(self.method_cache):
            self.log.error("missing %s"%self.method_cache)
            raise ex.excError
        import ConfigParser
        conf = ConfigParser.RawConfigParser()
        conf.read(self.method_cache)
        if not conf.has_section("methods"):
            self.log.error("missing 'methods' section of %s"%self.method_cache)
            raise ex.excError
        if not conf.has_option("methods", "feed"):
            self.log.error("missing 'feed' option in 'methods' section of %s"%self.method_cache)
            raise ex.excError
        if not conf.has_option("methods", "compliance"):
            self.log.error("missing 'compliance' option in 'methods' section of %s"%self.method_cache)
            raise ex.excError
        self.proxy_methods = conf.get("methods", "feed").split(',')
        self.comp_proxy_methods = conf.get("methods", "compliance").split(',')
        self.log.debug("%s loaded"%self.method_cache)
        self.log.debug("%d feed methods"%len(self.proxy_methods))
        self.log.debug("%d compliance methods"%len(self.comp_proxy_methods))

    def write_method_cache(self):
        import ConfigParser
        conf = ConfigParser.RawConfigParser()
        conf.add_section('methods')
        if len(self.proxy_methods) > 0:
            conf.set('methods', 'feed', ','.join(self.proxy_methods))
        if len(self.comp_proxy_methods) > 0:
            conf.set('methods', 'compliance', ','.join(self.comp_proxy_methods))
        f = open(self.method_cache, 'w')
        conf.write(f)
        self.log.debug("%s refreshed"%self.method_cache)

    def get_methods(self):
        self.log.debug("get method lists")
        try:
            if self.proxy is None:
                self.proxy = xmlrpclib.ServerProxy(rcEnv.dbopensvc)
            self.proxy_methods = self.proxy.system.listMethods()
        except:
            self.proxy = xmlrpclib.ServerProxy("https://127.0.0.1/")
            self.proxy_methods = []
        self.log.debug("%d feed methods"%len(self.proxy_methods))

        try:
            if self.comp_proxy is None:
                self.comp_proxy = xmlrpclib.ServerProxy(rcEnv.dbcompliance)
            self.comp_proxy_methods = self.comp_proxy.system.listMethods()
        except:
            self.comp_proxy = xmlrpclib.ServerProxy("https://127.0.0.1/")
            self.comp_proxy_methods = []
        self.log.debug("%d compliance methods"%len(self.comp_proxy_methods))

    def init(self, fn=None):
        if fn is not None:
            if fn in self.comp_fns:
                if self.comp_proxy is not None:
                    return
            elif self.proxy is not None:
                return

        self.setNodeEnv()
    
        try:
            a = socket.getaddrinfo(rcEnv.dbopensvc_host, None)
            if len(a) == 0:
                raise Exception
        except:
            self.log.error("could not resolve %s to an ip address. disable collector updates."%rcEnv.dbopensvc)

        socket.setdefaulttimeout(120)

        utils = __import__('rcUtilities'+rcEnv.sysname)

        if fn is None:
            try:
                if not utils.check_ping(rcEnv.dbopensvc_host):
                    self.log.error("could not ping %s. disable collector updates."%rcEnv.dbopensvc_host)
                    raise
                self.proxy = xmlrpclib.ServerProxy(rcEnv.dbopensvc)
            except:
                self.proxy = xmlrpclib.ServerProxy("https://127.0.0.1/")
            try:
                if not utils.check_ping(rcEnv.dbcompliance_host):
                    self.log.error("could not ping %s. disable collector updates."%rcEnv.dbcompliance_host)
                    raise
                self.comp_proxy = xmlrpclib.ServerProxy(rcEnv.dbcompliance)
            except:
                self.comp_proxy = xmlrpclib.ServerProxy("https://127.0.0.1/")
        elif fn not in self.comp_fns:
            try:
                if not utils.check_ping(rcEnv.dbopensvc_host):
                    self.log.error("could not ping %s. disable collector updates."%rcEnv.dbopensvc_host)
                    raise
                self.proxy = xmlrpclib.ServerProxy(rcEnv.dbopensvc)
            except:
                self.proxy = xmlrpclib.ServerProxy("https://127.0.0.1/")
        else:
            try:
                if not utils.check_ping(rcEnv.dbcompliance_host):
                    self.log.error("could not ping %s. disable collector updates."%rcEnv.dbopensvc_host)
                    raise
                self.comp_proxy = xmlrpclib.ServerProxy(rcEnv.dbcompliance)
            except:
                self.comp_proxy = xmlrpclib.ServerProxy("https://127.0.0.1/")

        self.log.debug("feed proxy %s"%str(self.proxy))
        self.log.debug("compliance proxy %s"%str(self.comp_proxy))

        try:
            self.load_method_cache()
        except ex.excError:
            self.get_methods()
            self.write_method_cache()

        if "register_node" not in self.proxy_methods:
            self.auth_node = False

    def stop_worker(self):
        if self.queue is None:
            self.log.debug("worker already stopped (queue is None)")
            return
        if self.worker is None:
            self.log.debug("worker already stopped (worker is None)")
            return
        if not self.worker.is_alive():
            self.log.debug("worker already stopped (not alive)")
            return
        self.log.debug("give poison pill to worker")
        self.queue.put(None)
        self.worker.join()
        self.queue = None
        self.worker = None

    def init_worker(self):
        if self._worker:
            return
       
        if self.worker is not None:
            return

        try:
            self.queue = Queue()
        except:
            self.log.error("Queue not supported. disable async mode")
            self.queue = None
            return
        self.worker = Process(target=call_worker, name="xmlrpc", args=(self.queue,))
        self.worker.start()
        self.log.debug("worker started")

    def begin_action(self, svc, action, begin, sync=True):
        try:
            import version
            version = version.version
        except:
            version = "0";
    
        args = [['svcname',
             'action',
             'hostname',
             'hostid',
             'version',
             'begin',
             'cron'],
            [repr(svc.svcname),
             repr(action),
             repr(rcEnv.nodename),
             repr(hostid),
             repr(version),
             repr(str(begin)),
             '1' if svc.cron else '0']
        ]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        if sync:
            self.proxy.begin_action(*args)
        else:
            self.submit("begin_action", *args)
    
    def end_action(self, svc, action, begin, end, logfile, sync=True):
        err = 'ok'
        dateprev = None
        lines = open(logfile, 'r').read()
        pids = set([])
    
        """Example logfile line:
        2009-11-11 01:03:25,252;;DISK.VG;;INFO;;unxtstsvc01_data is already up;;10200;;EOL
        """
        vars = ['svcname',
                'action',
                'hostname',
                'hostid',
                'pid',
                'begin',
                'end',
                'status_log',
                'status',
                'cron']
        vals = []
        for line in lines.split(';;EOL\n'):
            if line.count(';;') != 4:
                continue
            date = line.split(';;')[0]
    
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
                             res_err,
                             '1' if svc.cron else '0'])
    
            res_err = 'ok'
            (date, res, lvl, msg, pid) = line.split(';;')
    
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
                         res_err,
                         '1' if svc.cron else '0'])
    
        if len(vals) > 0:
            args = [vars, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            if sync:
                self.proxy.res_action_batch(*args)
            else:
                self.submit("res_action_batch", *args)
    
        """Complete the wrap-up database entry
        """
    
        """ If logfile is empty, default to current process pid
        """
        if len(pids) == 0:
            pids = set([os.getpid()])
    
        args = [
            ['svcname',
             'action',
             'hostname',
             'hostid',
             'pid',
             'begin',
             'end',
             'time',
             'status',
             'cron'],
            [repr(svc.svcname),
             repr(action),
             repr(rcEnv.nodename),
             repr(hostid),
             repr(','.join(map(str, pids))),
             repr(str(begin)),
             repr(str(end)),
             repr(str(end-begin)),
             repr(str(err)),
             '1' if svc.cron else '0']
        ]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        if sync:
            self.proxy.end_action(*args)
        else:
            self.submit("end_action", *args)
    
    def svcmon_update_combo(self, g_vars, g_vals, r_vars, r_vals, sync=True):
        if 'svcmon_update_combo' in self.proxy_methods:
            args = [g_vars, g_vals, r_vars, r_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            if sync:
                self.proxy.svcmon_update_combo(*args)
            else:
                self.submit("svcmon_update_combo", *args)
        else:
            args = [g_vars, g_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            if sync:
                self.proxy.svcmon_update(*args)
            else:
                self.submit("svcmon_update", *args)
            args = [r_vars, r_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            if sync:
                self.proxy.resmon_update(*args)
            else:
                self.submit("resmon_update", *args)
    
    def push_appinfo(self, svc, sync=True):
        if 'update_appinfo' not in self.proxy_methods:
            return

        vars = ['app_svcname',
                'app_launcher',
                'app_key',
                'app_value']
        vals = svc.resources_by_id['app'].info()
        if len(vals) == 0:
            return

        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.update_appinfo(*args)

    def push_service(self, svc, sync=True):
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
                'svc_guestos',
                'svc_ha']
    
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
                repr(' '.join(svc.autostart_node)),
                repr(svc.app),
                repr(svc.svcmode),
                repr(envfile(svc.svcname)),
                repr(version),
                repr(svc.drnoaction),
                repr(guestos),
                '1' if svc.ha else '0']
    
        if 'container' in svc.resources_by_id:
            container_info = svc.resources_by_id['container'].get_container_info()
            vars += ['svc_vcpus', 'svc_vmem']
            vals += [container_info['vcpus'],
                     container_info['vmem']]
    
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.update_service(*args)
    
    def delete_services(self, sync=True):
        args = [hostid]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.delete_services(*args)
    
    def push_disks(self, svc, sync=True):
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
    
        di = __import__('rcDiskInfo'+rcEnv.sysname)
        disks = di.diskInfo()
        disklist_cache = {}
    
        args = [svc.svcname, rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.delete_disks(*args)
    
        for d in svc.disklist():
            if disks.disk_id(d) is None or disks.disk_id(d) == "":
                """ no point pushing to db an empty entry
                """
                continue
            args = [['disk_id',
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
            ]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.register_disk(*args)
    
    def push_stats_fs_u(self, l, sync=True):
        args = [l[0], l[1]]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.insert_stats_fs_u(*args)
    
    def push_pkg(self, sync=True):
        p = __import__('rcPkg'+rcEnv.sysname)
        vars = ['pkg_nodename',
                'pkg_name',
                'pkg_version',
                'pkg_arch']
        vals = p.listpkg()
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.delete_pkg(*args)
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.insert_pkg(*args)
    
    def push_patch(self, sync=True):
        p = __import__('rcPkg'+rcEnv.sysname)
        vars = ['patch_nodename',
                'patch_num',
                'patch_rev']
        vals = p.listpatch()
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.delete_patch(*args)
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.insert_patch(*args)
    
    def push_stats(self, force=False, interval=None, stats_dir=None, stats_start=None, stats_end=None, sync=True):
        try:
            s = __import__('rcStats'+rcEnv.sysname)
        except ImportError:
            return
        sp = s.StatsProvider(interval=interval,
                             stats_dir=stats_dir,
                             stats_start=stats_start,
                             stats_end=stats_end)
        h = {}
        for stat in ['cpu', 'mem_u', 'proc', 'swap', 'block',
                     'blockdev', 'netdev', 'netdev_err']:
            h[stat] = sp.get(stat)
        import cPickle
        args = [cPickle.dumps(h)]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.insert_stats(*args)
    
    def push_asset(self, node, sync=True):
        try:
            m = __import__('rcAsset'+rcEnv.sysname)
        except ImportError:
            print "pushasset methods not implemented on", rcEnv.sysname
            return
        if "update_asset" not in self.proxy_methods:
            print "'update_asset' method is not exported by the collector"
            return
        d = m.Asset(node).get_asset_dict()
        args = [d.keys(), d.values()]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.update_asset(*args)
    
    def push_sym(self, sync=True):
        if 'update_sym_xml' not in self.proxy_methods:
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
            sym_proxy = ServerProxy(rcEnv.dbopensvc)
            args = [sym.sid, sym.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            sym_proxy.update_sym_xml(*args)
    
    def push_all(self, svcs, sync=True):
        args = [[svc.svcname for svc in svcs]]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.delete_service_list(*args)
        for svc in svcs:
            self.push_service(svc, sync=sync)
            self.push_appinfo(svc, sync=sync)
            self.push_disks(svc, sync=sync)
    
    def push_checks(self, vars, vals, sync=True):
        if "push_checks" not in self.proxy_methods:
            print "'push_checks' method is not exported by the collector"
            return
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.push_checks(*args)
    
    def register_node(self, sync=True):
        return self.proxy.register_node(rcEnv.nodename)
    
    def comp_get_moduleset_modules(self, moduleset, sync=True):
        args = [moduleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_moduleset_modules(*args)
    
    def comp_get_moduleset(self, sync=True):
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_moduleset(*args)
    
    def comp_attach_moduleset(self, moduleset, sync=True):
        args = [rcEnv.nodename, moduleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_attach_moduleset(*args)
    
    def comp_detach_moduleset(self, moduleset, sync=True):
        args = [rcEnv.nodename, moduleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_detach_moduleset(*args)
    
    def comp_get_ruleset(self, sync=True):
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_ruleset(*args)
    
    def comp_get_dated_ruleset(self, date, sync=True):
        args = [rcEnv.nodename, date]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_dated_ruleset(*args)
    
    def comp_attach_ruleset(self, ruleset, sync=True):
        args = [rcEnv.nodename, ruleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_attach_ruleset(*args)
    
    def comp_detach_ruleset(self, ruleset, sync=True):
        args = [rcEnv.nodename, ruleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_detach_ruleset(*args)
    
    def comp_list_ruleset(self, pattern='%', sync=True):
        args = [pattern, rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_list_rulesets(*args)
    
    def comp_list_moduleset(self, pattern='%', sync=True):
        args = [pattern]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_list_modulesets(*args)
    
    def comp_log_action(self, vars, vals, sync=True):
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_log_action(*args)


if __name__ == "__main__":
    x = Collector()
    x.init()
    print x.proxy_methods
    print x.comp_proxy_methods
