import os

import rcExceptions as ex
from rcUtilities import *


if rcEnv.sysname == "Windows":
    mp = False
else:
    try:
        from multiprocessing import Process, Queue, Lock
        mp = True
    except:
        mp = False

def svcmon_normal1(svc, queue=None):
    # don't schedule svcmon updates for encap services.
    # those are triggered by the master node
    status = svc.group_status()
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

    o = svc.svcmon_push_lists(status)
    _size = len(str(o))
    if queue is None or _size > 30000:
        # multiprocess Queue not supported, can't combine results
        g_vars, g_vals, r_vars, r_vals = svc.svcmon_push_lists(status)
        svc.node.collector.call('svcmon_update_combo', g_vars, g_vals, r_vars, r_vals)
    else:
        queue.put(svc.svcmon_push_lists(status))

def svcmon_normal(svcs):
    ps = []
    queues = {}

    for svc in svcs:
        if svc.encap:
            continue
        if not mp:
            svcmon_normal1(svc, None)
            continue
        try:
            queues[svc.svcname] = Queue(maxsize=32000)
        except:
            # some platform don't support Queue's synchronize (bug 3770)
            queues[svc.svcname] = None
        p = Process(target=svcmon_normal1, args=(svc, queues[svc.svcname]))
        p.start()
        ps.append(p)
    for p in ps:
        p.join()

    if mp:
        g_vals = []
        r_vals = []

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

