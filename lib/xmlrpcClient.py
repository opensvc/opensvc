from __future__ import print_function
import socket
import sys
socket.setdefaulttimeout(180)

kwargs = {}
if sys.hexversion >= 0x02070900:
    import ssl
    kwargs["context"] = ssl._create_unverified_context()

try:
    import xmlrpclib
except ImportError:
    import xmlrpc.client as xmlrpclib

try:
    import httplib
except ImportError:
    import http.client as httplib

def get_proxy(uri):
    try:
        return xmlrpclib.ServerProxy(uri, **kwargs)
    except Exception as e:
        if "__init__" in str(e):
            return xmlrpclib.ServerProxy(uri)

from datetime import datetime, timedelta
import time
import random
import os
import sys
from rcGlobalEnv import rcEnv
import rcStatus
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
log.setLevel(logging.INFO)

try:
    if sys.version_info[0] >= 3:
        from multiprocessing import queue as Queue, Process
    else:
        from multiprocessing import Queue, Process
    from Queue import Empty
    if rcEnv.sysname == 'Windows':
        from multiprocessing import set_executable
        set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
    mp = True
except:
    mp = False

def do_call(fn, args, kwargs, log, proxy, mode="synchronous"):
    tries = 5
    for i in range(tries):
        try:
            return _do_call(fn, args, kwargs, log, proxy, mode=mode)
        except Exception as e:
            s = str(e)
            if "retry" in s:
                # db table changed. retry immediately
                max_wait = 0
            elif "restart" in s or "Gateway" in s:
                # collector overload issues, retry after a random delay
                max_wait = 3
            else:
                # no need to retry at all there, unknown cause
                raise
        if max_wait > 0:
            time.sleep(random.random()*max_wait)
        log.warning("retry call %s on error %s" % (fn, str(e)))
    log.error("failed to call %s after %d tries" % (fn, tries))

def _do_call(fn, args, kwargs, log, proxy, mode="synchronous"):
    log.info("call remote function %s in %s mode"%(fn, mode))
    try:
        _b = datetime.now()
        buff = getattr(proxy, fn)(*args, **kwargs)
        _e = datetime.now()
        _d = _e - _b
        log.info("call %s done in %d.%03d seconds"%(fn, _d.seconds, _d.microseconds//1000))
        return buff
    except Exception as e:
        _e = datetime.now()
        _d = _e - _b
        err = str(e)
    log.error("call %s error after %d.%03d seconds: %s"%(fn, _d.seconds, _d.microseconds//1000, err))

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
            do_call(fn, args, kwargs, o.log, o.proxy, mode="asynchronous")
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
        try:
            import ConfigParser
        except ImportError:
            import configparser as ConfigParser
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
        else:
            rcEnv.dbcompliance_transport, rcEnv.dbcompliance_host, rcEnv.dbcompliance_port, rcEnv.dbcompliance_app = None, None, None, None
        if config.has_option('node', 'uuid'):
            rcEnv.uuid = config.get('node', 'uuid')
        else:
            rcEnv.uuid = ""
        del(config)

    def submit(self, fn, *args, **kwargs):
        self.init_worker()
        self.log.info("enqueue %s"%fn)
        self.queue.put((fn, args, kwargs), block=True)

    def call(self, *args, **kwargs):
        fn = args[0]
        self.init(fn)
        if rcEnv.dbopensvc == "None":
            return {"ret": 1, "msg": "no collector defined. set 'dbopensvc' in node.conf"}
        if len(self.proxy_methods) == 0:
            return
        if len(args) > 1:
            args = args[1:]
        else:
            args = []
        if fn == "register_node" and \
           'register_node' not in self.proxy_methods:
            print("collector does not support node registration", file=sys.stderr)
            return
        if rcEnv.uuid == "" and \
           rcEnv.dbopensvc != "None" and \
           not rcEnv.warned and \
           self.auth_node and \
           fn != "register_node":
            print("this node is not registered. try 'nodemgr register'", file=sys.stderr)
            print("to disable this warning, set 'dbopensvc = None' in node.conf", file=sys.stderr)
            rcEnv.warned = True
            return
        return do_call(fn, args, kwargs, self.log, self, mode="synchronous")

    def __init__(self, worker=False):
        self.proxy = None
        self.proxy_methods = []
        self.comp_proxy = None
        self.comp_proxy_methods = []

        self._worker = worker
        self.worker = None
        self.queue = None
        self.comp_fns = ['comp_get_data_moduleset',
                         'comp_get_svc_data_moduleset',
                         'comp_get_data',
                         'comp_get_svc_data',
                         'comp_attach_moduleset',
                         'comp_attach_svc_moduleset',
                         'comp_detach_moduleset',
                         'comp_detach_svc_moduleset',
                         'comp_get_ruleset',
                         'comp_get_svc_ruleset',
                         'comp_get_ruleset_md5',
                         'comp_attach_ruleset',
                         'comp_attach_svc_ruleset',
                         'comp_detach_ruleset',
                         'comp_detach_svc_ruleset',
                         'comp_list_ruleset',
                         'comp_list_moduleset',
                         'comp_show_status',
                         'comp_log_actions']
        self.auth_node = True
        self.log = logging.getLogger("xmlrpc%s"%('.worker' if worker else ''))

    def get_methods_dbopensvc(self):
        if rcEnv.dbopensvc == "None":
            self.proxy_methods = []
            return
        self.log.debug("get dbopensvc method list")
        try:
            if self.proxy is None:
                self.proxy = get_proxy(rcEnv.dbopensvc)
            self.proxy_methods = self.proxy.system.listMethods()
        except Exception as e:
            self.log.error(str(e))
            self.proxy = get_proxy("https://127.0.0.1/")
            self.proxy_methods = []
        self.log.debug("%d feed methods"%len(self.proxy_methods))

    def get_methods_dbcompliance(self):
        if rcEnv.dbcompliance == "None":
            self.comp_proxy_methods = []
            return
        self.log.debug("get dbcompliance method list")
        try:
            if self.comp_proxy is None:
                self.comp_proxy = get_proxy(rcEnv.dbcompliance)
            self.comp_proxy_methods = self.comp_proxy.system.listMethods()
        except Exception as e:
            self.log.error(str(e))
            self.comp_proxy = get_proxy("https://127.0.0.1/")
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

        if rcEnv.dbopensvc == "None":
            return

        try:
            a = socket.getaddrinfo(rcEnv.dbopensvc_host, None)
            if len(a) == 0:
                raise Exception
            dbopensvc_ip = a[0][-1][0]
        except:
            self.log.error("could not resolve %s to an ip address. disable collector updates."%rcEnv.dbopensvc_host)

        try:
            a = socket.getaddrinfo(rcEnv.dbcompliance_host, None)
            if len(a) == 0:
                raise Exception
            dbcompliance_ip = a[0][-1][0]
        except Exception as e:
            self.log.error(str(e))
            self.log.error("could not resolve %s to an ip address. disable collector updates."%rcEnv.dbcompliance_host)

        try:
            self.proxy = get_proxy(rcEnv.dbopensvc)
            self.get_methods_dbopensvc()
        except Exception as e:
            self.log.error(str(e))
            self.proxy = get_proxy("https://127.0.0.1/")

        if fn in self.comp_fns:
            try:
                self.comp_proxy = get_proxy(rcEnv.dbcompliance)
                self.get_methods_dbcompliance()
            except:
                self.comp_proxy = get_proxy("https://127.0.0.1/")

        self.log.info("feed proxy %s"%str(self.proxy))
        self.log.info("compliance proxy %s"%str(self.comp_proxy))

        if "register_node" not in self.proxy_methods:
            self.auth_node = False

    def stop_worker(self):
        if self.queue is None:
            self.log.debug("worker already stopped (queue is None)")
            return
        if self.worker is None:
            self.log.debug("worker already stopped (worker is None)")
            return
        try:
            if not self.worker.is_alive():
                self.log.debug("worker already stopped (not alive)")
                return
        except AssertionError:
            self.log.error("don't stop worker (not a child of this process)")
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
        except Exception as e:
            self.log.error("Queue not supported. disable async mode. %s" % str(e))
            self.queue = None
            return
        self.worker = Process(target=call_worker, name="xmlrpc", args=(self.queue,))
        self.worker.start()
        self.log.info("worker started")

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
        if sync or not mp:
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
            if ";;status_history;;" in line:
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
            if sync or not mp:
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
        if sync or not mp:
            self.proxy.end_action(*args)
        else:
            self.submit("end_action", *args)

    def svcmon_update_combo(self, g_vars, g_vals, r_vars, r_vals, sync=True):
        if 'svcmon_update_combo' in self.proxy_methods:
            args = [g_vars, g_vals, r_vars, r_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            if sync or not mp:
                self.proxy.svcmon_update_combo(*args)
            else:
                self.submit("svcmon_update_combo", *args)
        else:
            args = [g_vars, g_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            if sync or not mp:
                self.proxy.svcmon_update(*args)
            else:
                self.submit("svcmon_update", *args)
            args = [r_vars, r_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            if sync or not mp:
                self.proxy.resmon_update(*args)
            else:
                self.submit("resmon_update", *args)

    def _push_appinfo(self, svc, sync=True):
        if 'update_appinfo' not in self.proxy_methods:
            return

        vars = ['app_svcname',
                'app_nodename',
                'cluster_type',
                'app_launcher',
                'app_key',
                'app_value']
        vals = []
        for r in svc.get_resources():
            if not hasattr(r, "info"):
                continue
            try:
                vals += r.info()
            except Exception as e:
                print(e, file=sys.stderr)
        if len(vals) == 0:
            return

        for val in vals:
            print("%-16s %-20s %s"%(val[3], val[4], val[5]))

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

        vars = ['svc_hostid',
                'svc_name',
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
                'svc_drnoaction',
                'svc_ha']

        vals = [repr(hostid),
                repr(svc.svcname),
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
                repr(svc.drnoaction),
                '1' if svc.ha else '0']

        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.update_service(*args)

        vars = ['mon_svcname',
                'mon_nodname',
                'mon_vmname',
                'mon_guestos',
                'mon_vmem',
                'mon_vcpus',
                'mon_containerpath']
        vals = []

        for container in svc.get_resources('container'):
            container_info = container.get_container_info()
            vals += [[svc.svcname,
                      rcEnv.nodename,
                      container.name,
                      container.guestos if hasattr(container, 'guestos') and container.guestos is not None else "",
                      container_info['vmem'],
                      container_info['vcpus'],
                      container.zonepath if hasattr(container, 'zonepath') else ""]]

        if len(vals) > 0:
            args = [vars, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.svcmon_update(*args)

    def push_disks(self, node, sync=True):
        import re
        di = __import__('rcDiskInfo'+rcEnv.sysname)
        disks = di.diskInfo()
        try:
            m = __import__("rcDevTree"+rcEnv.sysname)
        except ImportError:
            return
        tree = m.DevTree()
        tree.load(di=disks)
        vars = ['disk_id',
                'disk_svcname',
                'disk_size',
                'disk_used',
                'disk_vendor',
                'disk_model',
                'disk_dg',
                'disk_nodename',
                'disk_region']
        vals = []

        # hash to add up disk usage across all services
        dh = {}
        served_disks = []

        svcs = node.svcs

        for svc in svcs:
            # hash to add up disk usage inside a service
            valsh = {}
            for r in svc.get_resources():
                if hasattr(r, "name"):
                    disk_dg = r.name
                elif hasattr(r, "dev"):
                    disk_dg = r.dev
                else:
                    disk_dg = r.rid

                if hasattr(r, 'devmap') and hasattr(r, 'vm_hostname'):
                    if hasattr(svc, "clustername"):
                        cluster = svc.clustername
                    else:
                        cluster = ','.join(sorted(list(svc.nodes)))
                    served_disks += map(lambda x: (x[0], r.vm_hostname()+'.'+x[1], cluster), r.devmap())

                try:
                    devpaths = r.devlist()
                except Exception as e:
                    print(e)
                    devpaths = []

                for devpath in devpaths:
                    for d, used, region in tree.get_top_devs_usage_for_devpath(devpath):
                        disk_id = disks.disk_id(d)
                        if disk_id is None or disk_id == "":
                            """ no point pushing to db an empty entry
                            """
                            continue
                        if disk_id.startswith(rcEnv.nodename+".loop"):
                            continue
                        disk_size = disks.disk_size(d)
                        if disk_id in dh:
                            dh[disk_id] += used
                        else:
                            dh[disk_id] = used
                        if dh[disk_id] > disk_size:
                            dh[disk_id] = disk_size

                        if disk_id not in valsh or used == disk_size:
                            valsh[disk_id] = [
                             disk_id,
                             svc.svcname,
                             disk_size,
                             used,
                             disks.disk_vendor(d),
                             disks.disk_model(d),
                             disk_dg,
                             rcEnv.nodename,
                             region
                            ]
                        elif disk_id in valsh and valsh[disk_id][3] < disk_size:
                            valsh[disk_id][3] += used
                            valsh[disk_id][6] = ""
                            valsh[disk_id][8] = ""

                        if valsh[disk_id][3] > disk_size:
                            valsh[disk_id][3] = disk_size

            for l in valsh.values():
                vals += [map(lambda x: repr(x), l)]
                print(l[1], "disk", l[0], "%d/%dM"%(l[3], l[2]), "region", region)

        done = []
        region = 0

        try:
            devpaths = node.devlist(tree)
        except Exception as e:
            print(e)
            devpaths = []

        for d in devpaths:
            disk_id = disks.disk_id(d)
            if disk_id is None or disk_id == "":
                """ no point pushing to db an empty entry
                """
                continue

            if disk_id.startswith(rcEnv.nodename+".loop"):
                continue

            if re.match(r"/dev/rdsk/.*s[01345678]", d):
                # don't report partitions
                continue

            # Linux Node:devlist() reports paths, so we can have duplicate
            # disks here.
            if disk_id in done:
                continue
            done.append(disk_id)

            disk_size = disks.disk_size(d)

            if disk_id in dh:
                left = disk_size - dh[disk_id]
            else:
                left = disk_size
            if left == 0:
                continue
            print(rcEnv.nodename, "disk", disk_id, "%d/%dM"%(left, disk_size), "region", region)
            vals.append([
                 repr(disk_id),
                 "",
                 repr(disk_size),
                 repr(left),
                 repr(disks.disk_vendor(d)),
                 repr(disks.disk_model(d)),
                 "",
                 repr(rcEnv.nodename),
                 repr(region)
            ])


        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.register_disks(*args)

        #
        # register disks this node provides to its VM
        #
        vars = ['disk_id',
                'disk_arrayid',
                'disk_devid',
                'disk_size',
                'disk_raid',
                'disk_group']
        vals = []

        for dev_id, vdisk_id, cluster in served_disks:
            disk_id = disks.disk_id(dev_id)
            try:
                disk_size = disks.disk_size(dev_id)
            except:
                continue
            vals.append([
              vdisk_id,
              cluster,
              disk_id,
              str(disk_size),
              "virtual",
              "virtual"
            ])
            print("register served disk", disk_id, "as", vdisk_id, "from varray", cluster)

        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.register_diskinfo(*args)

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
        n = len(vals)
        if n == 0:
            print("No package found. Skip push.")
            return
        else:
            print("Pushing %d packages information."%n)
        if len(vals[0]) >= 5:
            vars.append('pkg_type')
        if len(vals[0]) >= 6:
            vars.append('pkg_install_date')
        if len(vals[0]) >= 7:
            vars.append('pkg_sig')
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
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
        if len(vals) == 0:
            return
        if len(vals[0]) == 4:
            vars.append('patch_install_date')
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.insert_patch(*args)

    def push_stats(self, interval=None, stats_dir=None,
                   stats_start=None, stats_end=None, sync=True, disable=None):
        try:
            s = __import__('rcStats'+rcEnv.sysname)
        except ImportError:
            return

        try:
            sp = s.StatsProvider(interval=interval,
                                 stats_dir=stats_dir,
                                 stats_start=stats_start,
                                 stats_end=stats_end)
        except ValueError as e:
            print(str(e))
            return 1
        except Exception as e:
            print(e)
            raise
        h = {}
        for stat in ['cpu', 'mem_u', 'proc', 'swap', 'block',
                     'blockdev', 'netdev', 'netdev_err', 'svc', 'fs_u']:
            if disable is not None and stat in disable:
                print("%s collection is disabled in node configuration"%stat)
                continue
            h[stat] = sp.get(stat)
            print("%s stats: %d samples" % (stat, len(h[stat][1])))
        import cPickle
        args = [cPickle.dumps(h)]
        if self.auth_node:
             args += [(rcEnv.uuid, rcEnv.nodename)]
        print("pushing")
        self.proxy.insert_stats(*args)

    def sysreport_lstree(self, sync=True):
        args = []
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        data = self.proxy.sysreport_lstree(*args)
        return data

    def send_sysreport(self, fpath, deleted, sync=True):
        args = []
        if fpath is None:
            args += ["", ""]
        else:
            with open(fpath, 'rb') as f:
                binary = xmlrpclib.Binary(f.read())
            args = [os.path.basename(fpath), binary]
            print("archive length:", len(binary.data))
        args += [deleted]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.send_sysreport(*args)

    def push_asset(self, node, sync=True):
        try:
            m = __import__('rcAsset'+rcEnv.sysname)
        except ImportError:
            print("pushasset methods not implemented on", rcEnv.sysname)
            return
        if "update_asset" not in self.proxy_methods:
            print("'update_asset' method is not exported by the collector")
            return
        d = m.Asset(node).get_asset_dict()

        gen = {}
        if 'hba' in d:
            vars = ['nodename', 'hba_id', 'hba_type']
            vals = []
            for hba_id, hba_type in d['hba']:
               vals.append([rcEnv.nodename, hba_id, hba_type])
            del(d['hba'])
            gen.update({'hba': [vars, vals]})

        if 'targets' in d:
            import copy
            vars = ['hba_id', 'tgt_id']
            vals = copy.copy(d['targets'])
            del(d['targets'])
            gen.update({'targets': [vars, vals]})

        if 'lan' in d:
            vars = ['mac', 'intf', 'type', 'addr', 'mask', 'flag_deprecated']
            vals = []
            for mac, l in d['lan'].items():
                for _d in l:
                    vals.append([mac, _d['intf'], _d['type'], _d['addr'], _d['mask'], _d['flag_deprecated']])
            del(d['lan'])
            gen.update({'lan': [vars, vals]})

        if 'uids' in d:
            vars = ['user_name', 'user_id']
            vals = d['uids']
            del(d['uids'])
            gen.update({'uids': [vars, vals]})

        if 'gids' in d:
            vars = ['group_name', 'group_id']
            vals = d['gids']
            del(d['gids'])
            gen.update({'gids': [vars, vals]})

        if len(gen) > 0:
            args = [gen]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.insert_generic(*args)

        args = [d.keys(), d.values()]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        if node.options.syncrpc:
            self.proxy.update_asset_sync(*args)
        else:
            self.proxy.update_asset(*args)

    def push_brocade(self, objects=[], sync=True):
        if 'update_brocade' not in self.proxy_methods:
            print("'update_brocade' method is not exported by the collector")
            return
        m = __import__('rcBrocade')
        try:
            brocades = m.Brocades(objects)
        except:
            return
        for brocade in brocades:
            vals = []
            for key in brocade.keys:
                try:
                    vals.append(getattr(brocade, 'get_'+key)())
                except:
                    print("error fetching", key)
                    continue
            args = [brocade.name, brocade.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_brocade(*args)

    def push_vioserver(self, objects=[], sync=True):
        if 'update_vioserver' not in self.proxy_methods:
            print("'update_vioserver' method is not exported by the collector")
            return
        m = __import__('rcVioServer')
        try:
            vioservers = m.VioServers(objects)
        except:
            return
        for vioserver in vioservers:
            vals = []
            for key in vioserver.keys:
                vals.append(getattr(vioserver, 'get_'+key)())
            args = [vioserver.name, vioserver.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_vioserver(*args)

    def push_hds(self, objects=[], sync=True):
        if 'update_hds' not in self.proxy_methods:
            print("'update_hds' method is not exported by the collector")
            return
        m = __import__('rcHds')
        try:
            hdss = m.Hdss(objects)
        except:
            return
        for hds in hdss:
            vals = []
            for key in hds.keys:
                vals.append(getattr(hds, 'get_'+key)())
            args = [hds.name, hds.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_hds(*args)

    def push_necism(self, objects=[], sync=True):
        if 'update_necism' not in self.proxy_methods:
            print("'update_necism' method is not exported by the collector")
            return
        m = __import__('rcNecIsm')
        try:
            necisms = m.NecIsms(objects)
        except:
            return
        for necism in necisms:
            vals = []
            for key in necism.keys:
                vals.append(getattr(necism, 'get_'+key)())
            args = [necism.name, necism.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_necism(*args)

    def push_hp3par(self, objects=[], sync=True):
        if 'update_hp3par' not in self.proxy_methods:
            print("'update_hp3par' method is not exported by the collector")
            return
        m = __import__('rcHp3par')
        try:
            hp3pars = m.Hp3pars(objects)
        except:
            return
        for hp3par in hp3pars:
            vals = []
            for key in hp3par.keys:
                vals.append(getattr(hp3par, 'get_'+key)())
            args = [hp3par.name, hp3par.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_hp3par(*args)

    def push_centera(self, objects=[], sync=True):
        if 'update_centera' not in self.proxy_methods:
            print("'update_centera' method is not exported by the collector")
            return
        m = __import__('rcCentera')
        try:
            centeras = m.Centeras(objects)
        except:
            return
        for centera in centeras:
            vals = []
            print(centera.name)
            for key in centera.keys:
                print(" extract", key)
                vals.append(getattr(centera, 'get_'+key)())
            args = [centera.name, [k+".xml" for k in centera.keys], vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_centera(*args)

    def push_emcvnx(self, objects=[], sync=True):
        if 'update_emcvnx' not in self.proxy_methods:
            print("'update_emcvnx' method is not exported by the collector")
            return
        m = __import__('rcEmcVnx')
        try:
            emcvnxs = m.EmcVnxs(objects)
        except:
            return
        for emcvnx in emcvnxs:
            vals = []
            print(emcvnx.name)
            for key in emcvnx.keys:
                print(" extract", key)
                vals.append(getattr(emcvnx, 'get_'+key)())
            args = [emcvnx.name, emcvnx.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_emcvnx(*args)

    def push_netapp(self, objects=[], sync=True):
        if 'update_netapp' not in self.proxy_methods:
            print("'update_netapp' method is not exported by the collector")
            return
        m = __import__('rcNetapp')
        try:
            netapps = m.Netapps(objects)
        except:
            return
        for netapp in netapps:
            vals = []
            print(netapp.name)
            for key in netapp.keys:
                print(" extract", key)
                vals.append(getattr(netapp, 'get_'+key)())
            args = [netapp.name, netapp.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_netapp(*args)

    def push_ibmsvc(self, objects=[], sync=True):
        if 'update_ibmsvc' not in self.proxy_methods:
            print("'update_ibmsvc' method is not exported by the collector")
            return
        m = __import__('rcIbmSvc')
        try:
            ibmsvcs = m.IbmSvcs(objects)
        except:
            return
        for ibmsvc in ibmsvcs:
            vals = []
            for key in ibmsvc.keys:
                vals.append(getattr(ibmsvc, 'get_'+key)())
            args = [ibmsvc.name, ibmsvc.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_ibmsvc(*args)

    def push_nsr(self, sync=True):
        if 'update_nsr' not in self.proxy_methods:
           print("'update_nsr' method is not exported by the collector")
           return
        m = __import__('rcNsr')
        try:
            nsr = m.Nsr()
        except:
            return
        vals = []
        for key in nsr.keys:
            vals.append(getattr(nsr, 'get_'+key)())
        args = [rcEnv.nodename, nsr.keys, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        try:
            self.proxy.update_nsr(*args)
        except:
            print("error pushing nsr index")

    def push_ibmds(self, objects=[], sync=True):
        if 'update_ibmds' not in self.proxy_methods:
           print("'update_ibmds' method is not exported by the collector")
           return
        m = __import__('rcIbmDs')
        try:
            ibmdss = m.IbmDss(objects)
        except:
            return
        for ibmds in ibmdss:
            vals = []
            for key in ibmds.keys:
                vals.append(getattr(ibmds, 'get_'+key)())
            args = [ibmds.name, ibmds.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            try:
                self.proxy.update_ibmds(*args)
            except:
                print("error pushing", ibmds.name)

    def push_gcedisks(self, objects=[], sync=True):
        if 'update_gcedisks' not in self.proxy_methods:
           print("'update_gcedisks' method is not exported by the collector")
           return
        m = __import__('rcGceDisks')
        try:
            arrays = m.GceDiskss(objects)
        except:
            return
        for array in arrays:
            vals = []
            for key in array.keys:
                vals.append(getattr(array, 'get_'+key)())
            args = [array.name, array.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            try:
                self.proxy.update_gcedisks(*args)
            except Exception as e:
                print("error pushing %s: %s" % (array.name, str(e)))

    def push_freenas(self, objects=[], sync=True):
        if 'update_freenas' not in self.proxy_methods:
           print("'update_freenas' method is not exported by the collector")
           return
        m = __import__('rcFreenas')
        try:
            arrays = m.Freenass(objects)
        except:
            return
        for array in arrays:
            vals = []
            for key in array.keys:
                vals.append(getattr(array, 'get_'+key)())
            args = [array.name, array.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            try:
                self.proxy.update_freenas(*args)
            except:
                print("error pushing", array.name)

    def push_dcs(self, objects=[], sync=True):
        if 'update_dcs' not in self.proxy_methods:
           print("'update_dcs' method is not exported by the collector")
           return
        m = __import__('rcDcs')
        try:
            dcss = m.Dcss(objects)
        except:
            return
        for dcs in dcss:
            vals = []
            for key in dcs.keys:
                vals.append(getattr(dcs, 'get_'+key)())
            args = [dcs.name, dcs.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            try:
                self.proxy.update_dcs(*args)
            except:
                print("error pushing", dcs.name)

    def push_eva(self, objects=[], sync=True):
        if 'update_eva_xml' not in self.proxy_methods:
            print("'update_eva_xml' method is not exported by the collector")
            return
        m = __import__('rcEva')
        try:
            evas = m.Evas(objects)
        except:
            return
        for eva in evas:
            vals = []
            for key in eva.keys:
                vals.append(getattr(eva, 'get_'+key)())
            args = [eva.name, eva.keys, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_eva_xml(*args)

    def push_sym(self, objects=[], sync=True):
        import zlib
        if 'update_sym_xml' not in self.proxy_methods:
            print("'update_sym_xml' method is not exported by the collector")
            return 1
        m = __import__('rcSymmetrix')
        try:
            syms = m.Syms(objects)
        except Exception as e:
            print(e)
            return 1
        r = 0
        for sym in syms:
            # can be too big for a single rpc
            print(sym.sid)
            for key in sym.keys:
                print(" extract", key)
                vars = [key]
                vals = [xmlrpclib.Binary(zlib.compress(getattr(sym, 'get_'+key)()))]
                args = [sym.sid, vars, vals]
                if self.auth_node:
                    args += [(rcEnv.uuid, rcEnv.nodename)]
                try:
                    print(" send   ", key)
                    self.proxy.update_sym_xml(*args)
                except Exception as e:
                    print(sym.sid, key, ":", e)
                    r = 1
                    continue
            # signal all files are received
            args = [sym.sid, [], []]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.update_sym_xml(*args)
        return r

    def push_all(self, svcs, sync=True):
        args = [[svc.svcname for svc in svcs]]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        for svc in svcs:
            self.push_service(svc, sync=sync)

    def push_appinfo(self, svcs, sync=True):
        args = [[svc.svcname for svc in svcs]]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        for svc in svcs:
            self._push_appinfo(svc, sync=sync)

    def push_checks(self, vars, vals, sync=True):
        if "push_checks" not in self.proxy_methods:
            print("'push_checks' method is not exported by the collector")
            return
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.push_checks(*args)

    def register_node(self, sync=True):
        return self.proxy.register_node(rcEnv.nodename)

    def comp_get_data(self, modulesets=[], sync=True):
        args = [rcEnv.nodename, modulesets]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_data(*args)

    def comp_get_svc_data(self, svcname, modulesets=[], sync=True):
        args = [rcEnv.nodename, svcname, modulesets]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_svc_data(*args)

    def comp_get_data_moduleset(self, sync=True):
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_data_moduleset(*args)

    def comp_get_svc_data_moduleset(self, svc, sync=True):
        args = [svc]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_svc_data_moduleset(*args)

    def comp_attach_moduleset(self, moduleset, sync=True):
        args = [rcEnv.nodename, moduleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_attach_moduleset(*args)

    def comp_attach_svc_moduleset(self, svc, moduleset, sync=True):
        args = [svc, moduleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_attach_svc_moduleset(*args)

    def comp_detach_svc_moduleset(self, svcname, moduleset, sync=True):
        args = [svcname, moduleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_detach_svc_moduleset(*args)

    def comp_detach_moduleset(self, moduleset, sync=True):
        args = [rcEnv.nodename, moduleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_detach_moduleset(*args)

    def comp_get_svc_ruleset(self, svcname, sync=True):
        args = [svcname]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_svc_ruleset(*args)

    def comp_get_ruleset(self, sync=True):
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_ruleset(*args)

    def comp_get_ruleset_md5(self, rset_md5, sync=True):
        args = [rset_md5]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_ruleset_md5(*args)

    def comp_attach_ruleset(self, ruleset, sync=True):
        args = [rcEnv.nodename, ruleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_attach_ruleset(*args)

    def comp_detach_svc_ruleset(self, svcname, ruleset, sync=True):
        args = [svcname, ruleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_detach_svc_ruleset(*args)

    def comp_attach_svc_ruleset(self, svcname, ruleset, sync=True):
        args = [svcname, ruleset]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_attach_svc_ruleset(*args)

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

    def comp_log_actions(self, vars, vals, sync=True):
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_log_actions(*args)

    def comp_show_status(self, svcname, pattern='%', sync=True):
        args = [svcname, pattern]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_show_status(*args)

    def collector_update_root_pw(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_update_root_pw(*args)

    def collector_ack_unavailability(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_ack_unavailability(*args)

    def collector_list_unavailability_ack(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_list_unavailability_ack(*args)

    def collector_list_actions(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_list_actions(*args)

    def collector_ack_action(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_ack_action(*args)

    def collector_status(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_status(*args)

    def collector_asset(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_asset(*args)

    def collector_networks(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_networks(*args)

    def collector_checks(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_checks(*args)

    def collector_disks(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_disks(*args)

    def collector_alerts(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_alerts(*args)

    def collector_show_actions(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_show_actions(*args)

    def collector_events(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_events(*args)

    def collector_tag(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_tag(*args)

    def collector_untag(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_untag(*args)

    def collector_create_tag(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_create_tag(*args)

    def collector_show_tags(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_show_tags(*args)

    def collector_list_tags(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_list_tags(*args)

    def collector_list_nodes(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_list_nodes(*args)

    def collector_list_services(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_list_services(*args)

    def collector_list_filtersets(self, opts, sync=True):
        args = [opts]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_list_filtersets(*args)

    def collector_get_action_queue(self, sync=True):
        args = [rcEnv.nodename]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_get_action_queue(*args)

    def collector_update_action_queue(self, data, sync=True):
        args = [data]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.proxy.collector_update_action_queue(*args)


if __name__ == "__main__":
    x = Collector()
    x.init()
    print(x.proxy_methods)
    print(x.comp_proxy_methods)
