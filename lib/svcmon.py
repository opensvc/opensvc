from __future__ import print_function
import sys
import os
import optparse
import string
import platform

#
# add project lib to path
#
prog = "svcmon"

import svcBuilder
import rcExceptions as ex
from rcUtilities import *
from lock import *
import node
import rcStatus
import rcColor

sysname, nodename, x, x, machine, x = platform.uname()

if sysname == "Windows":
    mp = False
else:
    try:
        from multiprocessing import Process, Queue, Lock
        mp = True
    except:
        mp = False

try:
    from version import version
except:
    version = "dev"

def max_len(svcs):
    _len = 7
    for svc in svcs:
        if type(svc) == dict:
            svcname = svc.get("svcname", "")
        else:
            svcname = svc.svcname
        l = len(svcname)
        if _len < l:
            _len = l
        if not hasattr(svc, "get_resources"):
            continue
        for container in svc.get_resources('container'):
            l = len(getattr(container, "name")) + 2
            if _len < l:
                _len = l
    return _len

def svcmon_normal1(svc,upddb=False, fmt=None, queue=None, lock=None):
    # don't schedule svcmon updates for encap services.
    # those are triggered by the master node
    status = svc.group_status()
    l = []
    applen = 10
    app = str(svc.app)
    if len(app) > applen:
        app = app[:applen-1]+"*"
    name = svc.svcname
    name = rcColor.colorize(fmt.split()[0] % name, rcColor.color.BOLD)
    data = [
              name,
              app,
              svc.svc_env,
              svc.clustertype,
              '-',
              "yes" if svc.frozen() else "no",
              "yes" if svc.disabled else "no",
              rcStatus.colorize_status(status["avail"]),
              rcStatus.colorize_status(status["overall"]),
    ]
    if options.verbose:
        data += [
              rcStatus.colorize_status(status["container"]),
              rcStatus.colorize_status(status["ip"]),
              rcStatus.colorize_status(status["disk"]),
              rcStatus.colorize_status(status["fs"]),
              rcStatus.colorize_status(status.get("share", "n/a")),
              rcStatus.colorize_status(status["app"]),
              rcStatus.colorize_status(status["hb"]),
              rcStatus.colorize_status(status["sync"]),
        ]
    buff = fmt % tuple(data)
    l.append(buff)
    containers = svc.get_resources("container")
    if len(containers) > 0 and svc.has_encap_resources:
        for container in containers:
            try:
                s = svc.encap_json_status(container)
            except ex.excNotAvailable as e:
                s = {'resources': [],
                     'ip': 'n/a',
                     'disk': 'n/a',
                     'sync': 'n/a',
                     'hb': 'n/a',
                     'container': 'n/a',
                     'fs': 'n/a',
                     'share': 'n/a',
                     'app': 'n/a',
                     'avail': 'n/a',
                     'overall': 'n/a'}

            name = " @"+container.name
            name = rcColor.colorize(fmt.split()[0] % name, rcStatus.color.WHITE)
            data = [
                      name,
                      '-',
                      '-',
                      '-',
                      container.type.replace('container.', ''),
                      '-',
                      '-',
                      rcStatus.colorize_status(s["avail"]),
                      rcStatus.colorize_status(s["overall"]),
            ]
            if options.verbose:
                data += [
                      rcStatus.colorize_status(s["container"]),
                      rcStatus.colorize_status(s["ip"]),
                      rcStatus.colorize_status(s["disk"]),
                      rcStatus.colorize_status(s["fs"]),
                      rcStatus.colorize_status(s.get("share", "n/a")),
                      rcStatus.colorize_status(s["app"]),
                      rcStatus.colorize_status(s["hb"]),
                      rcStatus.colorize_status(s["sync"]),
                ]
            buff = fmt % tuple(data)
            l.append(buff)

    if lock is not None:
        lock.acquire()
    print('\n'.join(l))
    if lock is not None:
        lock.release()

    if upddb:
        o = svc.svcmon_push_lists(status)
        _size = len(str(o))
        if queue is None or _size > 30000:
            # multiprocess Queue not supported, can't combine results
            g_vars, g_vals, r_vars, r_vals = svc.svcmon_push_lists(status)
            svc.node.collector.call('svcmon_update_combo', g_vars, g_vals, r_vars, r_vals)
        else:
            queue.put(svc.svcmon_push_lists(status))

def svcmon_cluster(node):
    svcnames = ",".join([r.svcname for r in node.svcs])
    try:
        data = node.collector_rest_get("/services?props=svc_id,svcname,svc_app,svc_env,svc_cluster_type,svc_status,svc_availstatus,svc_status_updated&meta=0&orderby=svcname&filters=svcname (%s)"%svcnames)
    except Exception as e:
        print(e, file=sys.stderr)
        return
    if "error" in data:
        print("error fetching data from the collector rest api: %s" % data["error"], file=sys.stderr)
        return 1
    if "data" not in data:
        print("no 'data' key in the collector rest api response", file=sys.stderr)
        return 1
    if len(data["data"]) == 0:
        print("no service found on the collector", file=sys.stderr)
        return 1
    svc_ids = []
    for d in data["data"]:
       svc_ids.append(d["svc_id"])

    max_len_data = []
    max_len_data += data["data"]
    if options.verbose:
        instance_data = svcmon_cluster_verbose_data(node, svc_ids)
        for instances in instance_data.values():
            max_len_data += instances

    svcname_len = max_len(max_len_data)
    fmt_svcname = '%(svcname)-' + str(svcname_len) + 's'
    fmt = fmt_svcname + ' %(svc_app)-10s %(svc_env)-4s %(svc_cluster_type)-8s | %(svc_availstatus)-10s %(svc_status)-10s | %(svc_status_updated)s'
    print(" "*svcname_len+" app        type topology | avail      overall    | updated")
    print(" "*svcname_len+" -------------------------+-----------------------+--------------------")

    for d in data["data"]:
       d["svcname"] = rcColor.colorize(fmt_svcname % d, rcStatus.color.BOLD)
       d["svc_status"] = rcStatus.colorize_status(d["svc_status"])
       d["svc_availstatus"] = rcStatus.colorize_status(d["svc_availstatus"])
       print(fmt % d)
       if options.verbose:
           if d['svc_id'] not in instance_data:
               print(" (no instances data)")
               continue
           for inst in instance_data[d["svc_id"]]:
               print(fmt%inst)

def svcmon_cluster_verbose_data(node, svc_ids):
    data = node.collector_rest_get("/services_instances?props=svc_id,node_id,mon_availstatus,mon_overallstatus,mon_updated&meta=0&filters=svc_id (%s)"%",".join(svc_ids))
    if "error" in data:
        print("error fetching data from the collector rest api: %s" % data["error"], file=sys.stderr)
        return {}
    if "data" not in data:
        print("no 'data' key in the collector rest api response", file=sys.stderr)
        return {}
    if len(data["data"]) == 0:
        print("no service instance found on the collector", file=sys.stderr)
        return {}
    _data = {}

    node_ids = set([])
    for d in data["data"]:
        node_ids.add(d["node_id"])

    node_data = node.collector_rest_get("/nodes?props=node_id,nodename&meta=0&filters=node_id (%s)"%",".join(node_ids))
    if "error" in node_data:
        print("error fetching data from the collector rest api: %s" % data["error"], file=sys.stderr)
        return {}
    if "data" not in node_data:
        print("no 'data' key in the collector rest api response", file=sys.stderr)
        return {}
    if len(node_data["data"]) == 0:
        print("no node found on the collector", file=sys.stderr)
        return {}

    nodenames = {}
    for d in node_data["data"]:
        nodenames[d["node_id"]] = d["nodename"]

    for d in data["data"]:
        if d["svc_id"] not in _data:
            _data[d["svc_id"]] = []
        d["svc_app"] = ""
        d["svc_cluster_type"] = ""
        d["svc_env"] = ""
        d["svc_availstatus"] = rcStatus.colorize_status(d["mon_availstatus"])
        d["svc_status"] = rcStatus.colorize_status(d["mon_overallstatus"])
        d["svc_status_updated"] = d["mon_updated"]
        if d["node_id"] in nodenames:
            nodename = nodenames[d["node_id"]]
        else:
            nodename = d["node_id"]
        d["svcname"] = " @"+nodename
        _data[d["svc_id"]].append(d)
    return _data

def svcmon_normal(svcs, upddb=False):
    svcname_len = max_len(svcs)
    fmt_svcname = '%-' + str(svcname_len) + 's'
    if options.verbose:
        fmt = fmt_svcname + ' %-10s %-4s %-8s %-9s | %-6s %-8s | %-10s %-10s | %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s'
        print(" "*svcname_len+" app        type topology container | frozen disabled | avail      overall    | container  ip         disk       fs         share      app        hb         sync")
        print(" "*svcname_len+" -----------------------------------+-----------------+-----------------------+----------------------------------------------------------------------------------")
    else:
        fmt = fmt_svcname + ' %-10s %-4s %-8s %-9s | %-6s %-8s | %-10s %-10s'
        print(" "*svcname_len+" app        type topology container | frozen disabled | avail      overall    ")
        print(" "*svcname_len+" -----------------------------------+-----------------+-----------------------")

    ps = []
    queues = {}
    try:
        lock = Lock()
    except:
        lock = None

    for svc in svcs:
        if svc.encap and upddb:
            continue
        if not mp:
            svcmon_normal1(svc, upddb, fmt, None)
            continue
        try:
            queues[svc.svcname] = Queue(maxsize=32000)
        except:
            # some platform don't support Queue's synchronize (bug 3770)
            queues[svc.svcname] = None
        p = Process(target=svcmon_normal1, args=(svc, upddb, fmt, queues[svc.svcname], lock))
        p.start()
        ps.append(p)
    for p in ps:
        p.join()

    if upddb and mp:
        g_vals = []
        r_vals = []
        if options.delay > 0:
            import random
            import time
            delay = int(random.random()*options.delay)
            time.sleep(delay)

        for svc in svcs:
            if svc.svcname not in queues or queues[svc.svcname] is None:
                continue
            if queues[svc.svcname].empty():
                continue
            g_vars, _g_vals, r_vars, _r_vals = queues[svc.svcname].get()
            g_vals.append(_g_vals)
            r_vals.append(_r_vals)
        if len(g_vals) > 0:
            svc.node.collector.call('svcmon_update_combo', g_vars, g_vals, r_vars, r_vals)

__ver = prog + " version " + version
__usage = prog + " [ OPTIONS ]\n"
parser = optparse.OptionParser(version=__ver, usage=__usage)
parser.add_option("-s", "--service", default="", action="store", dest="parm_svcs",
                  help="comma-separated list of service to display status of")
parser.add_option("--refresh", default=False, action="store_true", dest="refresh",
                  help="do not use resource status cache")
parser.add_option("--updatedb", default=False, action="store_true", dest="upddb",
                  help="update resource status in central database")
parser.add_option("-v", "--verbose", default=False, action="store_true", dest="verbose",
                  help="display resource groups status for each selected service")
parser.add_option("--maxdelaydb", default=0, action="store", type="int", dest="delay",
                  help="introduce a random delay before pushing to database to level the load on the collector")
parser.add_option("--debug", default=False, action="store_true", dest="debug",
                  help="debug mode")
parser.add_option("-c", "--cluster", default=False, action="store_true", dest="cluster",
                  help="fetch and display cluster-wide service status from the collector.")
parser.add_option("--color", default="auto", action="store", dest="color",
                  help="colorize output. possible values are : auto=guess based on tty presence, always|yes=always colorize, never|no=never colorize")

(options, args) = parser.parse_args()
rcColor.use_color = options.color

node = node.Node()

def main():
    if options.upddb:
        lockf = 'svcmon.lock'
        try:
            lockfd = monlock(fname=lockf)
        except ex.excError:
            return 1
        except:
            import traceback
            traceback.print_exc()
            return 1
 

    if len(options.parm_svcs) > 0:
        node.build_services(svcnames=options.parm_svcs.split(','))
    else:
        node.build_services()

    node.set_rlimit()

    for s in node.svcs:
        s.options.debug = options.debug
        s.options.refresh = options.upddb
        if options.refresh:
            s.options.refresh = options.refresh

    if options.cluster:
        svcmon_cluster(node)
    else:
        svcmon_normal(node.svcs, options.upddb)

    node.close()

    if options.upddb:
        try:
            monunlock(lockfd)
        except ex.excError:
            return 1
        except:
            import traceback
            traceback.print_exc()
            return 1

    return 0

if __name__ == "__main__":
    try:
        r = main()
    except ex.excError as e:
        print(e, file=sys.stderr)
        r = 1
    sys.exit(r)

