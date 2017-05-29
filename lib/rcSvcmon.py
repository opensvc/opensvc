import os

import rcExceptions as ex
from rcUtilities import *
from rcGlobalEnv import rcEnv


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
    o = svc.svcmon_push_lists()
    _size = len(str(o))
    if queue is None or _size > 30000:
        # multiprocess Queue not supported, can't combine results
        g_vars, g_vals, r_vars, r_vals = o
        svc.node.collector.call('svcmon_update_combo', g_vars, g_vals, r_vars, r_vals)
    else:
        queue.put(o)

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

