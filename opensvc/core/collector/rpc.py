from __future__ import print_function

import logging
import logging.handlers
import os
import random
import socket
import sys
import time
from datetime import datetime

import core.exceptions as ex
import foreign.six as six
from env import Env
from utilities.naming import split_path

socket.setdefaulttimeout(5)

kwargs = {}
try:
    import ssl
    kwargs["context"] = ssl._create_unverified_context()
    kwargs["allow_none"] = True
except:
    pass

try:
    import xmlrpclib
except ImportError:
    import xmlrpc.client as xmlrpclib

def get_proxy(uri):
    try:
        return xmlrpclib.ServerProxy(uri, **kwargs)
    except Exception as e:
        if "__init__" in str(e):
            return xmlrpclib.ServerProxy(uri)



Env.warned = False

logfile = os.path.join(Env.paths.pathlog, 'xmlrpc.log')
log = logging.getLogger("xmlrpc")
log.setLevel(logging.INFO)

LO_ADDR = "127.0.0.1"
DUMMY_URL = "https://" + LO_ADDR

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
    s = ""
    for i in range(tries):
        try:
            return _do_call(fn, args, kwargs, log, proxy, mode=mode)
        except socket.timeout:
            max_wait = 1
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
        log.warning("retry call %s on error %s" % (fn, s))
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
    except (OSError, Exception) as exc:
        # socket.gaierror (name resolution failure) is a subclass of OSError in py3.3+
        _e = datetime.now()
        _d = _e - _b
        log.error("call %s error after %d.%03d seconds: %s"%(fn, _d.seconds, _d.microseconds//1000, exc))
        if hasattr(exc, "faultString"):
            raise ex.Error(getattr(exc, "faultString").split(":", 1)[-1])
        else:
            raise ex.Error(str(exc))

class CollectorRpc(object):
    def call(self, *args, **kwargs):
        fn = args[0]
        self.init(fn)
        if self.node.collector_env.dbopensvc is None:
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
        if self.node.collector_env.uuid == "" and \
           self.node.collector_env.dbopensvc is not None and \
           not Env.warned and \
           fn != "register_node":
            print("this node is not registered. try 'om node register'", file=sys.stderr)
            print("to disable this warning, set 'dbopensvc = None' in node.conf", file=sys.stderr)
            Env.warned = True
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
        self.log = logging.getLogger("xmlrpc")

    def get_methods_dbopensvc(self):
        if self.node.collector_env.dbopensvc is None:
            self.proxy_methods = []
            return
        self.log.debug("get dbopensvc method list")
        try:
            if self.proxy is None:
                self.proxy = get_proxy(self.node.collector_env.dbopensvc)
            self.proxy_methods = self.proxy.system.listMethods()
        except Exception as exc:
            self.log.error("get dbopensvc methods: %s", exc)
            self.proxy = get_proxy(DUMMY_URL)
            self.proxy_methods = []
        self.log.debug("%d feed methods"%len(self.proxy_methods))

    def get_methods_dbcompliance(self):
        if self.node.collector_env.dbcompliance is None:
            self.comp_proxy_methods = []
            return
        self.log.debug("get dbcompliance method list")
        try:
            if self.comp_proxy is None:
                self.comp_proxy = get_proxy(self.node.collector_env.dbcompliance)
            self.comp_proxy_methods = self.comp_proxy.system.listMethods()
        except Exception as exc:
            self.log.error("get dbcompliance methods: %s", exc)
            self.comp_proxy = get_proxy(DUMMY_URL)
            self.comp_proxy_methods = []
        self.log.debug("%d compliance methods"%len(self.comp_proxy_methods))

    def init(self, fn=None):
        if fn is not None:
            if fn in self.comp_fns:
                if self.comp_proxy is not None:
                    return
            elif self.proxy is not None:
                return

        if self.node.collector_env.dbopensvc is None:
            return

        self.init_feed_proxy()
        if fn in self.comp_fns:
            self.init_comp_proxy()

    def init_feed_proxy(self):
        try:
            a = socket.getaddrinfo(self.node.collector_env.dbopensvc_host, None)
            if len(a) == 0:
                raise Exception
            dbopensvc_ip = a[0][-1][0]
        except Exception:
            self.log.error("could not resolve %s to an ip address. disable collector updates."%self.node.collector_env.dbopensvc_host)
            self.proxy = get_proxy(DUMMY_URL)
            return
        try:
            self.proxy = get_proxy(self.node.collector_env.dbopensvc)
            self.get_methods_dbopensvc()
        except Exception as exc:
            self.log.error("init dbopensvc: %s", exc)
            self.proxy = get_proxy(DUMMY_URL)
            return
        self.log.info("feed proxy %s"%str(self.proxy))

    def init_comp_proxy(self):
        try:
            a = socket.getaddrinfo(self.node.collector_env.dbcompliance_host, None)
            if len(a) == 0:
                raise Exception
            dbcompliance_ip = a[0][-1][0]
        except Exception:
            self.log.error("could not resolve %s to an ip address. disable collector updates."%self.node.collector_env.dbcompliance_host)
            self.comp_proxy = get_proxy(DUMMY_URL)
            return
        try:
            self.comp_proxy = get_proxy(self.node.collector_env.dbcompliance)
            self.get_methods_dbcompliance()
        except:
            self.comp_proxy = get_proxy(DUMMY_URL)
            return
        self.log.info("compliance proxy %s"%str(self.comp_proxy))

    def disable(self):
        self.proxy = None

    def disabled(self):
        """
        Example repr():
            <ServerProxy for 127.0.0.1/RPC2>
        """
        return self.proxy is None or LO_ADDR in repr(self.proxy)

    def reinit(self):
        if self.disabled():
            self.log.info("disabled")
            self.init()
            if not self.disabled():
                self.log.info("the collector is available. proxies reinitialized")
                return True
        return False

    def begin_action(self, svcname, action, version, begin, cron):
        args = [['svcname',
             'action',
             'hostname',
             'version',
             'begin',
             'cron'],
            [str(svcname),
             str(action),
             str(Env.nodename),
             str(version),
             str(begin),
             '1' if cron else '0']
        ]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        self.proxy.begin_action(*args)

    def end_action(self, path, action, begin, end, cron, alogfile):
        err = 'ok'
        res = None
        res_err = None
        pid = None
        msg = None
        name, namespace, kind = split_path(path)
        with open(alogfile, 'r') as ofile:
            lines = ofile.read()
        try:
            os.unlink(alogfile)
        except Exception:
            pass
        pids = set()

        """Example logfile line:
        2009-11-11 01:03:25,252;;DISK.VG;;INFO;;unxtstsvc01_data is already up;;10200;;EOL
        """
        vars = ["svcname",
                "action",
                "hostname",
                "pid",
                "begin",
                "end",
                "status_log",
                "status",
                "cron"]
        vals = []
        last = []
        for line in lines.split(";;EOL\n"):
            if line.count(";;") != 4:
                continue
            if ";;status_history;;" in line:
                continue
            date = line.split(";;")[0]

            res_err = "ok"
            date, res, lvl, msg, pid = line.split(";;")
            res = res.lower().replace(Env.nodename+"."+kind+"."+name, "").replace(Env.nodename, "").lstrip(".")
            res_action = res + " " + action
            res_action = res_action.strip()
            date = date.split(",")[0]

            # database overflow protection
            trim_lim = 10000
            trim_tag = " <trimmed> "
            trim_head = trim_lim // 2
            trim_tail = trim_head-len(trim_tag)
            if len(msg) > trim_lim:
                msg = msg[:trim_head]+" <trimmed> "+msg[-trim_tail:]

            pids |= set([pid])
            if lvl is None or lvl == "DEBUG":
                continue
            elif lvl == "ERROR":
                err = "err"
                res_err = "err"
            elif lvl == "WARNING" and err != "err":
                err = "warn"
            elif lvl == "WARNING" and res_err != "err":
                res_err = "warn"

            try:
                if last:
                    if last[3] == pid and last[1] == res_action and last[7] == res_err:
                        last[6] += "\n"+msg
                        continue
                    else:
                        vals.append(last)
            except Exception as exc:
                print(exc)
                continue
            
            last = [
                path,
                res_action,
                Env.nodename,
                pid,
                date,
                "",
                msg,
                res_err,
                "1" if cron else "0"
            ]

        if last:
            vals.append(last)

        if len(vals) > 0:
            args = [vars, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
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
            [str(path),
             str(action),
             str(Env.nodename),
             ','.join(map(str, pids)),
             begin,
             end,
             str(duration.seconds),
             str(err),
             '1' if cron else '0']
        ]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        self.proxy.end_action(*args)

    def svcmon_update_combo(self, g_vars, g_vals, r_vars, r_vals):
        args = [g_vars, g_vals, r_vars, r_vals]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        self.proxy.svcmon_update_combo(*args)

    def push_resinfo(self, vals, sync=False):
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
        args += [(self.node.collector_env.uuid, Env.nodename)]
        if sync:
            self.proxy.update_resinfo_sync(*args)
        else:
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
                'cluster_id',
                'svc_topology',
                'svc_flex_min_nodes',
                'svc_flex_max_nodes',
                'svc_flex_target',
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

        vals = [svc.path,
                svc.node.cluster_id,
                svc.topology,
                svc.flex_min,
                svc.flex_max,
                svc.flex_target,
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
        args += [(self.node.collector_env.uuid, Env.nodename)]
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
            vals += [[svc.path,
                      Env.nodename,
                      container.vm_hostname,
                      container.guestos if hasattr(container, 'guestos') and container.guestos is not None else "",
                      container_info['vmem'],
                      container_info['vcpus'],
                      container.zonepath if hasattr(container, 'zonepath') else ""]]

        if len(vals) > 0:
            args = [vars, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
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
                 disk["dg"],
                 Env.nodename,
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
                 disk["dg"],
                 Env.nodename,
                 0,
            ])

        args = [vars, vals]
        args += [(self.node.collector_env.uuid, Env.nodename)]
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
        args += [(self.node.collector_env.uuid, Env.nodename)]
        self.proxy.register_diskinfo(*args)

    def push_stats_fs_u(self, l, sync=True):
        args = [l[0], l[1]]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        self.proxy.insert_stats_fs_u(*args)

    def push_pkg(self, sync=True):
        import utilities.packages.list as p
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
        n_fields = len(vals[0])
        if n_fields >= 5:
            vars.append('pkg_type')
        if n_fields >= 6:
            vars.append('pkg_install_date')
        if n_fields >= 7:
            vars.append('pkg_sig')
        args = [vars, vals]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        self.proxy.insert_pkg(*args)

    def push_patch(self, sync=True):
        import utilities.packages.list as p
        vars = ['patch_nodename',
                'patch_num',
                'patch_rev']
        vals = p.listpatch()
        if len(vals) == 0:
            return
        if len(vals[0]) == 4:
            vars.append('patch_install_date')
        args = [vars, vals]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        self.proxy.insert_patch(*args)

    def push_stats(self, interval=None, stats_dir=None,
                   stats_start=None, stats_end=None, sync=True, disable=None):
        try:
            from utilities.stats.provider import StatsProvider
        except ImportError:
            return

        try:
            sp = StatsProvider(interval=interval,
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
        args += [(self.node.collector_env.uuid, Env.nodename)]
        print("pushing")
        self.proxy.insert_stats(*args)

    def sysreport_lstree(self, sync=True):
        args = []
        args += [(self.node.collector_env.uuid, Env.nodename)]
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
        args += [(self.node.collector_env.uuid, Env.nodename)]
        self.proxy.send_sysreport(*args)

    def push_asset(self, node, data=None, sync=True):
        if "update_asset" not in self.proxy_methods:
            print("'update_asset' method is not exported by the collector")
            return
        d = dict(data)

        gen = {}
        if 'hardware' in d:
            gen["hardware"] = d["hardware"]
            del(d['hardware'])

        if 'hba' in d:
            vars = ['nodename', 'hba_id', 'hba_type']
            vals = [(Env.nodename, _d["hba_id"], _d["hba_type"]) for _d in d['hba']]
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
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.insert_generic(*args)

        _vars = []
        _vals = []
        for key, _d in d.items():
            _vars.append(key)
            if _d["value"] is None:
                _d["value"] = ""
            _vals.append(_d["value"])
        args = [_vars, _vals]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        if node.options.syncrpc:
            self.proxy.update_asset_sync(*args)
        else:
            self.proxy.update_asset(*args)

    def daemon_ping(self, sync=True):
        args = [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.daemon_ping(*args)

    def push_status(self, svcname, data, sync=True):
        import json
        args = [svcname, json.dumps(data), (self.node.collector_env.uuid, Env.nodename)]
        self.proxy.push_status(*args)

    def push_daemon_status(self, data, changes=None, sync=True):
        import json
        args = [json.dumps(data), json.dumps(changes), (self.node.collector_env.uuid, Env.nodename)]
        self.proxy.push_daemon_status(*args)

    def push_brocade(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_brocade' not in self.proxy_methods:
            print("'update_brocade' method is not exported by the collector")
            return
        import drivers.sanswitch.brocade as m
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
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_brocade(*args)

    def push_vioserver(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_vioserver' not in self.proxy_methods:
            print("'update_vioserver' method is not exported by the collector")
            return
        import drivers.array.vioserver as m
        try:
            vioservers = m.VioServers(objects)
        except:
            return
        for vioserver in vioservers:
            vals = []
            for key in vioserver.keys:
                vals.append(getattr(vioserver, 'get_'+key)())
            args = [vioserver.name, vioserver.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_vioserver(*args)

    def push_hds(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_hds' not in self.proxy_methods:
            print("'update_hds' method is not exported by the collector")
            return
        import drivers.array.hds as m
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
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_hds(*args)

    def push_necism(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_necism' not in self.proxy_methods:
            print("'update_necism' method is not exported by the collector")
            return
        import drivers.array.necism as m
        try:
            necisms = m.NecIsms(objects)
        except:
            return
        for necism in necisms:
            vals = []
            for key in necism.keys:
                vals.append(getattr(necism, 'get_'+key)())
            args = [necism.name, necism.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_necism(*args)

    def push_hp3par(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_hp3par' not in self.proxy_methods:
            print("'update_hp3par' method is not exported by the collector")
            return
        import drivers.array.hp3par as m
        try:
            hp3pars = m.Hp3pars(objects)
        except:
            return
        for hp3par in hp3pars:
            vals = []
            for key in hp3par.keys:
                vals.append(getattr(hp3par, 'get_'+key)())
            args = [hp3par.name, hp3par.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_hp3par(*args)

    def push_centera(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_centera' not in self.proxy_methods:
            print("'update_centera' method is not exported by the collector")
            return
        import drivers.array.centera as m
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
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_centera(*args)

    def push_emcvnx(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_emcvnx' not in self.proxy_methods:
            print("'update_emcvnx' method is not exported by the collector")
            return
        import drivers.array.emcvnx as m
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
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_emcvnx(*args)

    def push_netapp(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_netapp' not in self.proxy_methods:
            print("'update_netapp' method is not exported by the collector")
            return
        import drivers.array.netapp as m
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
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_netapp(*args)

    def push_ibmsvc(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_ibmsvc' not in self.proxy_methods:
            print("'update_ibmsvc' method is not exported by the collector")
            return
        import drivers.array.ibmsvc as m
        try:
            ibmsvcs = m.IbmSvcs(objects)
        except:
            return
        for ibmsvc in ibmsvcs:
            vals = []
            for key in ibmsvc.keys:
                vals.append(getattr(ibmsvc, 'get_'+key)())
            args = [ibmsvc.name, ibmsvc.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_ibmsvc(*args)

    def push_nsr(self, sync=True):
        if 'update_nsr' not in self.proxy_methods:
           print("'update_nsr' method is not exported by the collector")
           return
        import drivers.backupsrv.networker as m
        try:
            nsr = m.Nsr()
        except:
            return
        vals = []
        for key in nsr.keys:
            vals.append(getattr(nsr, 'get_'+key)())
        args = [Env.nodename, nsr.keys, vals]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        try:
            self.proxy.update_nsr(*args)
        except:
            print("error pushing nsr index")

    def push_ibmds(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_ibmds' not in self.proxy_methods:
           print("'update_ibmds' method is not exported by the collector")
           return
        import drivers.array.ibmds as m
        try:
            ibmdss = m.IbmDss(objects)
        except:
            return
        for ibmds in ibmdss:
            vals = []
            for key in ibmds.keys:
                vals.append(getattr(ibmds, 'get_'+key)())
            args = [ibmds.name, ibmds.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            try:
                self.proxy.update_ibmds(*args)
            except:
                print("error pushing", ibmds.name)

    def push_gcedisks(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_gcedisks' not in self.proxy_methods:
           print("'update_gcedisks' method is not exported by the collector")
           return
        import drivers.array.gce as m
        try:
            arrays = m.GceDiskss(objects)
        except:
            return
        for array in arrays:
            vals = []
            for key in array.keys:
                vals.append(getattr(array, 'get_'+key)())
            args = [array.name, array.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            try:
                self.proxy.update_gcedisks(*args)
            except Exception as e:
                print("error pushing %s: %s" % (array.name, str(e)))

    def push_freenas(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_freenas' not in self.proxy_methods:
           print("'update_freenas' method is not exported by the collector")
           return
        import drivers.array.freenas as m
        try:
            arrays = m.Freenass(objects)
        except:
            return
        for array in arrays:
            vals = []
            for key in array.keys:
                vals.append(getattr(array, 'get_'+key)())
            args = [array.name, array.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            try:
                self.proxy.update_freenas(*args)
            except:
                print("error pushing", array.name)

    def push_xtremio(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_xtremio' not in self.proxy_methods:
           print("'update_xtremio' method is not exported by the collector")
           return
        import drivers.array.xtremio as m
        try:
            arrays = m.Arrays(objects)
        except:
            return
        for array in arrays:
            vals = []
            for key in array.keys:
                vals.append(getattr(array, 'get_'+key)())
            args = [array.name, array.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            try:
                self.proxy.update_xtremio(*args)
            except Exception as exc:
                print("error pushing", array.name, file=sys.stderr)
                print(exc, file=sys.stderr)
                raise ex.Error

    def push_eva(self, objects=None, sync=True):
        if objects is None:
            objects = []
        if 'update_eva_xml' not in self.proxy_methods:
            print("'update_eva_xml' method is not exported by the collector")
            return
        import drivers.array.eva as m
        try:
            evas = m.Evas(objects)
        except:
            return
        for eva in evas:
            vals = []
            for key in eva.keys:
                vals.append(getattr(eva, 'get_'+key)())
            args = [eva.name, eva.keys, vals]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_eva_xml(*args)

    def push_hcs(self, objects=None, sync=True):
        if objects is None:
            objects = []
        import json
        import drivers.array.hcs as m
        try:
            arrays = m.Hcss(objects)
        except Exception as e:
            print(e)
            return 1
        r = 0
        try:
            for array in arrays:
                # can be too big for a single rpc
                print(array.name)
                for key in array.keys:
                    print(" extract", key)
                    vars = [key]
                    try:
                        data = getattr(array, 'get_'+key)()
                        vals = [json.dumps(data)]
                    except Exception as e:
                        print(e)
                        continue
                    args = [array.name, vars, vals]
                    args += [(self.node.collector_env.uuid, Env.nodename)]
                    try:
                        print(" send   ", key)
                        self.proxy.update_hcs(*args)
                    except Exception as e:
                        print(array.name, key, ":", e)
                        r = 1
                        continue
                # signal all files are received
                args = [array.name, [], []]
                args += [(self.node.collector_env.uuid, Env.nodename)]
                self.proxy.update_hcs(*args)
        finally:
            for array in arrays:
                array.close_session()
        return r

    def push_dorado(self, objects=None, sync=True):
        if objects is None:
            objects = []
        import json
        import drivers.array.dorado as m
        try:
            arrays = m.Dorados(objects)
        except Exception as e:
            print(e)
            return 1
        r = 0
        try:
            for array in arrays:
                # can be too big for a single rpc
                print(array.name)
                for key in array.keys:
                    print(" extract", key)
                    vars = [key]
                    try:
                        data = getattr(array, 'get_'+key)()
                        vals = [json.dumps(data)]
                    except Exception as e:
                        print(e)
                        continue
                    args = [array.name, vars, vals]
                    args += [(self.node.collector_env.uuid, Env.nodename)]
                    try:
                        print(" send   ", key)
                        self.proxy.update_dorado_xml(*args)
                    except Exception as e:
                        print(array.name, key, ":", e)
                        r = 1
                        continue
                # signal all files are received
                args = [array.name, [], []]
                args += [(self.node.collector_env.uuid, Env.nodename)]
                self.proxy.update_dorado_xml(*args)
        finally:
            for array in arrays:
                array.close_session()
        return r

    def push_sym(self, objects=None, sync=True):
        if objects is None:
            objects = []
        import zlib
        if 'update_sym_xml' not in self.proxy_methods:
            print("'update_sym_xml' method is not exported by the collector")
            return 1
        import drivers.array.symmetrix as m
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
                    data = getattr(sym, 'get_'+key)()
                    if six.PY3:
                        data = bytes(data, "utf-8")
                    vals = [xmlrpclib.Binary(zlib.compress(data))]
                except Exception as e:
                    print(e)
                    continue
                args = [sym.sid, vars, vals]
                args += [(self.node.collector_env.uuid, Env.nodename)]
                try:
                    print(" send   ", key)
                    self.proxy.update_sym_xml(*args)
                except Exception as e:
                    print(sym.sid, key, ":", e)
                    r = 1
                    continue
            # signal all files are received
            args = [sym.sid, [], []]
            args += [(self.node.collector_env.uuid, Env.nodename)]
            self.proxy.update_sym_xml(*args)
        return r

    def push_checks(self, data, sync=True):
        if "push_checks" not in self.proxy_methods:
            print("'push_checks' method is not exported by the collector")
            return
        vars = [\
            "chk_nodename",
            "chk_svcname",
            "chk_type",
            "chk_instance",
            "chk_value",
            "chk_updated"]
        vals = []
        now = str(datetime.now())
        for chk_type, d in data.items():
            for instance in d:
                vals.append([\
                    Env.nodename,
                    instance["path"],
                    chk_type,
                    instance['instance'],
                    str(instance['value']).replace("%", ""),
                    now]
                )
        self.proxy.push_checks(vars, vals, (self.node.collector_env.uuid, Env.nodename))

    def register_node(self, sync=True):
        return self.proxy.register_node(Env.nodename)

    def comp_get_data(self, modulesets=None, sync=True):
        if modulesets is None:
            modulesets = []
        args = [Env.nodename, modulesets]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_get_data_v2(*args)

    def comp_get_svc_data(self, svcname, modulesets=None, sync=True):
        if modulesets is None:
            modulesets = []
        args = [Env.nodename, svcname, modulesets]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_get_svc_data_v2(*args)

    def comp_get_data_moduleset(self, sync=True):
        args = [Env.nodename]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_get_data_moduleset(*args)

    def comp_get_svc_data_moduleset(self, svc, sync=True):
        args = [svc]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_get_svc_data_moduleset(*args)

    def comp_attach_moduleset(self, moduleset, sync=True):
        args = [Env.nodename, moduleset]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_attach_moduleset(*args)

    def comp_attach_svc_moduleset(self, svc, moduleset, sync=True):
        args = [svc, moduleset]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_attach_svc_moduleset(*args)

    def comp_detach_svc_moduleset(self, svcname, moduleset, sync=True):
        args = [svcname, moduleset]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_detach_svc_moduleset(*args)

    def comp_detach_moduleset(self, moduleset, sync=True):
        args = [Env.nodename, moduleset]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_detach_moduleset(*args)

    def comp_get_svc_ruleset(self, svcname, sync=True):
        args = [svcname]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_get_svc_ruleset(*args)

    def comp_get_ruleset(self, sync=True):
        args = [Env.nodename]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_get_ruleset(*args)

    def comp_get_ruleset_md5(self, rset_md5, sync=True):
        args = [rset_md5]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_get_ruleset_md5(*args)

    def comp_attach_ruleset(self, ruleset, sync=True):
        args = [Env.nodename, ruleset]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_attach_ruleset(*args)

    def comp_detach_svc_ruleset(self, svcname, ruleset, sync=True):
        args = [svcname, ruleset]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_detach_svc_ruleset(*args)

    def comp_attach_svc_ruleset(self, svcname, ruleset, sync=True):
        args = [svcname, ruleset]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_attach_svc_ruleset(*args)

    def comp_detach_ruleset(self, ruleset, sync=True):
        args = [Env.nodename, ruleset]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_detach_ruleset(*args)

    def comp_list_ruleset(self, pattern='%', sync=True):
        args = [pattern, Env.nodename]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_list_rulesets(*args)

    def comp_list_moduleset(self, pattern='%', sync=True):
        args = [pattern]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_list_modulesets(*args)

    def comp_log_actions(self, vars, vals, sync=True):
        args = [vars, vals]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_log_actions(*args)

    def comp_show_status(self, svcname, pattern='%', sync=True):
        args = [svcname, pattern]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.comp_proxy.comp_show_status(*args)

    def collector_update_root_pw(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_update_root_pw(*args)

    def collector_ack_unavailability(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_ack_unavailability(*args)

    def collector_list_unavailability_ack(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_list_unavailability_ack(*args)

    def collector_list_actions(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_list_actions(*args)

    def collector_ack_action(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_ack_action(*args)

    def collector_status(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_status(*args)

    def collector_asset(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_asset(*args)

    def collector_networks(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_networks(*args)

    def collector_checks(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_checks(*args)

    def collector_disks(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_disks(*args)

    def collector_alerts(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_alerts(*args)

    def collector_show_actions(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_show_actions(*args)

    def collector_events(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_events(*args)

    def collector_tag(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_tag(*args)

    def collector_untag(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_untag(*args)

    def collector_create_tag(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_create_tag(*args)

    def collector_show_tags(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_show_tags(*args)

    def collector_list_tags(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_list_tags(*args)

    def collector_list_nodes(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_list_nodes(*args)

    def collector_list_services(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_list_services(*args)

    def collector_list_filtersets(self, opts, sync=True):
        args = [opts]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_list_filtersets(*args)

    def collector_get_action_queue(self, sync=True):
        args = [Env.nodename]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_get_action_queue(*args)

    def collector_update_action_queue(self, data, sync=True):
        args = [data]
        args += [(self.node.collector_env.uuid, Env.nodename)]
        return self.proxy.collector_update_action_queue(*args)


if __name__ == "__main__":
    x = CollectorRpc()
    x.init()
    print(x.proxy_methods)
    print(x.comp_proxy_methods)
