from __future__ import print_function
import socket
import sys
import os
socket.setdefaulttimeout(180)

kwargs = {}
try:
    import ssl
    kwargs["context"] = ssl._create_unverified_context()
except:
    pass

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

rcEnv.warned = False

import logging
import logging.handlers
logfile = os.path.join(rcEnv.paths.pathlog, 'xmlrpc.log')
log = logging.getLogger("xmlrpc")
log.setLevel(logging.INFO)

try:
    fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    filehandler = logging.handlers.RotatingFileHandler(os.path.join(logfile),
                                                       maxBytes=5242880,
                                                       backupCount=5)
    filehandler.setFormatter(fileformatter)
    log.addHandler(filehandler)
except Exception as e:
    pass

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
        log.exception("call %s error after %d.%03d seconds"%(fn, _d.seconds, _d.microseconds//1000))

class Collector(object):
    def call(self, *args, **kwargs):
        fn = args[0]
        self.init(fn)
        if rcEnv.dbopensvc is None:
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
           rcEnv.dbopensvc is not None and \
           not rcEnv.warned and \
           self.auth_node and \
           fn != "register_node":
            print("this node is not registered. try 'nodemgr register'", file=sys.stderr)
            print("to disable this warning, set 'dbopensvc = None' in node.conf", file=sys.stderr)
            rcEnv.warned = True
            return
        return do_call(fn, args, kwargs, self.log, self, mode="synchronous")

    def __init__(self, node=None):
        self.node = node
        self.proxy = None
        self.proxy_methods = []
        self.comp_proxy = None
        self.comp_proxy_methods = []

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
        self.log = logging.getLogger("xmlrpc")

    def get_methods_dbopensvc(self):
        if rcEnv.dbopensvc is None:
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
        if rcEnv.dbcompliance is None:
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

        if rcEnv.dbopensvc is None:
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

    def begin_action(self, svcname, action, version, begin, cron):
        args = [['svcname',
             'action',
             'hostname',
             'version',
             'begin',
             'cron'],
            [str(svcname),
             str(action),
             str(rcEnv.nodename),
             str(version),
             str(begin),
             '1' if cron else '0']
        ]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.begin_action(*args)

    def end_action(self, svcname, action, begin, end, cron, alogfile):
        err = 'ok'
        dateprev = None
        res = None
        res_err = None
        pid = None
        msg = None
        lines = open(alogfile, 'r').read()
        pids = set()

        """Example logfile line:
        2009-11-11 01:03:25,252;;DISK.VG;;INFO;;unxtstsvc01_data is already up;;10200;;EOL
        """
        vars = ['svcname',
                'action',
                'hostname',
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
            if res is not None and dateprev is not None:
                res = res.lower()
                res = res.replace(rcEnv.nodename+'.'+svcname+'.','')
                vals.append([svcname,
                             res+' '+action,
                             rcEnv.nodename,
                             pid,
                             dateprev,
                             date,
                             msg,
                             res_err,
                             '1' if cron else '0'])

            res_err = 'ok'
            (date, res, lvl, msg, pid) = line.split(';;')

            # database overflow protection
            trim_lim = 10000
            trim_tag = ' <trimmed> '
            trim_head = trim_lim // 2
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
            res = res.replace(rcEnv.nodename+'.'+svcname+'.','')
            vals.append([svcname,
                         res+' '+action,
                         rcEnv.nodename,
                         pid,
                         dateprev,
                         date,
                         msg,
                         res_err,
                         '1' if cron else '0'])

        if len(vals) > 0:
            args = [vars, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.res_action_batch(*args)

        """Complete the wrap-up database entry
        """

        """ If logfile is empty, default to current process pid
        """
        if len(pids) == 0:
            pids = set([os.getpid()])

        duration = datetime.strptime(end, "%Y-%m-%d %H:%M:%S") - \
                   datetime.strptime(begin, "%Y-%m-%d %H:%M:%S")
        args = [
            ['svcname',
             'action',
             'hostname',
             'pid',
             'begin',
             'end',
             'time',
             'status',
             'cron'],
            [str(svcname),
             str(action),
             str(rcEnv.nodename),
             ','.join(map(str, pids)),
             begin,
             end,
             str(duration.seconds),
             str(err),
             '1' if cron else '0']
        ]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.end_action(*args)
        os.unlink(alogfile)

    def svcmon_update_combo(self, g_vars, g_vals, r_vars, r_vals):
        if 'svcmon_update_combo' in self.proxy_methods:
            args = [g_vars, g_vals, r_vars, r_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.svcmon_update_combo(*args)
        else:
            args = [g_vars, g_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.svcmon_update(*args)
            args = [r_vars, r_vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.resmon_update(*args)

    def push_resinfo(self, vals, sync=True):
        if 'update_resinfo' not in self.proxy_methods:
            return
        vars = ['res_svcname',
                'res_nodename',
                'topology',
                'rid',
                'res_key',
                'res_value']
        if len(vals) == 0:
            return
        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.update_resinfo(*args)

    def push_config(self, svc, sync=True):
        def repr_config(svc):
            import codecs
            if not os.path.exists(svc.paths.cf):
                return
            with codecs.open(svc.paths.cf, 'r', encoding="utf8") as f:
                buff = f.read()
                return buff
            return

        vars = ['svc_name',
                'svc_topology',
                'svc_flex_min_nodes',
                'svc_flex_max_nodes',
                'svc_flex_cpu_low_threshold',
                'svc_flex_cpu_high_threshold',
                'svc_env',
                'svc_nodes',
                'svc_drpnode',
                'svc_drpnodes',
                'svc_comment',
                'svc_app',
                'svc_config',
                'svc_ha']

        vals = [svc.svcname,
                svc.topology,
                svc.flex_min_nodes,
                svc.flex_max_nodes,
                svc.flex_cpu_low_threshold,
                svc.flex_cpu_high_threshold,
                svc.svc_env,
                ' '.join(svc.nodes),
                svc.drpnode,
                ' '.join(svc.drpnodes),
                svc.comment,
                svc.app,
                repr_config(svc),
                '1' if svc.ha else '0']

        args = [vars, vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.update_service(*args)

    def push_containerinfo(self, svc, sync=True):
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
                      container.vm_hostname,
                      container.guestos if hasattr(container, 'guestos') and container.guestos is not None else "",
                      container_info['vmem'],
                      container_info['vcpus'],
                      container.zonepath if hasattr(container, 'zonepath') else ""]]

        if len(vals) > 0:
            args = [vars, vals]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.svcmon_update(*args)

    def push_disks(self, data, sync=True):
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
        served_disks = []

        for disk_id, disk in data["disks"].items():
            for svcname, service in disk["services"].items():
                vals.append([
                 disk_id,
                 svcname,
                 disk["size"],
                 service["used"],
                 disk["vendor"],
                 disk["model"],
                 service["dg"],
                 rcEnv.nodename,
                 service["region"],
            ])
            if disk["used"] < disk["size"]:
                vals.append([
                 disk_id,
                 "",
                 disk["size"],
                 disk["size"] - disk["used"],
                 disk["vendor"],
                 disk["model"],
                 "",
                 rcEnv.nodename,
                 0,
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

        for disk_id, disk in data["served_disks"].items():
            vals.append([
              disk["vdisk_id"],
              disk["cluster"],
              disk_id,
              disk["size"],
              "virtual",
              "virtual"
            ])

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
        import json
        args = [json.dumps(h)]
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

    def push_asset(self, node, data=None, sync=True):
        if "update_asset" not in self.proxy_methods:
            print("'update_asset' method is not exported by the collector")
            return
        d = dict(data)

        gen = {}
        if 'hba' in d:
            vars = ['nodename', 'hba_id', 'hba_type']
            vals = [(rcEnv.nodename, _d["hba_id"], _d["hba_type"]) for _d in d['hba']]
            del(d['hba'])
            gen.update({'hba': [vars, vals]})

        if 'targets' in d:
            vars = ['hba_id', 'tgt_id']
            vals = [(_d["hba_id"], _d["tgt_id"]) for _d in d['targets']]
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
            vals = [(_d["username"], _d["uid"]) for _d in d['uids']]
            del(d['uids'])
            gen.update({'uids': [vars, vals]})

        if 'gids' in d:
            vars = ['group_name', 'group_id']
            vals = [(_d["groupname"], _d["gid"]) for _d in d['gids']]
            del(d['gids'])
            gen.update({'gids': [vars, vals]})

        if len(gen) > 0:
            args = [gen]
            if self.auth_node:
                args += [(rcEnv.uuid, rcEnv.nodename)]
            self.proxy.insert_generic(*args)

        _vars = []
        _vals = []
        for key, _d in d.items():
            _vars.append(key)
            if _d["value"] is None:
                _d["value"] = ""
            _vals.append(_d["value"])
        args = [_vars, _vals]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        if node.options.syncrpc:
            self.proxy.update_asset_sync(*args)
        else:
            self.proxy.update_asset(*args)

    def daemon_ping(self, sync=True):
        args = [(rcEnv.uuid, rcEnv.nodename)]
        self.proxy.daemon_ping(*args)

    def push_daemon_status(self, data, changes=None, sync=True):
        import json
        args = [json.dumps(data), json.dumps(changes), (rcEnv.uuid, rcEnv.nodename)]
        self.proxy.push_daemon_status(*args)

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
            hdss = m.Arrays(objects)
        except Exception as e:
            print(e, file=sys.stderr)
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

    def push_xtremio(self, objects=[], sync=True):
        if 'update_xtremio' not in self.proxy_methods:
           print("'update_xtremio' method is not exported by the collector")
           return
        m = __import__('rcXtremio')
        try:
            arrays = m.Arrays(objects)
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
                self.proxy.update_xtremio(*args)
            except Exception as exc:
                print("error pushing", array.name, file=sys.stderr)
                print(exc, file=sys.stderr)
                raise ex.excError

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
            syms = m.Arrays(objects)
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
                try:
                    vals = [xmlrpclib.Binary(zlib.compress(getattr(sym, 'get_'+key)()))]
                except Exception as e:
                    print(e)
                    continue
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
            self.push_config(svc, sync=sync)
            self.push_containerinfo(svc, sync=sync)

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
        return self.comp_proxy.comp_get_data_v2(*args)

    def comp_get_svc_data(self, svcname, modulesets=[], sync=True):
        args = [rcEnv.nodename, svcname, modulesets]
        if self.auth_node:
            args += [(rcEnv.uuid, rcEnv.nodename)]
        return self.comp_proxy.comp_get_svc_data_v2(*args)

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
