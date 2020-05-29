from __future__ import print_function

import datetime
import json
import os
import time

from env import Env
from foreign.winstats import *


def collect(_=None):
    now = datetime.datetime.now()
    data = {
        "ts": str(now),
    }

    meminfo = get_mem_info()
    data["mem"] = {
        "tp": meminfo.TotalPhys // 1024,
        "ap": meminfo.AvailPhys // 1024,
        "ts": meminfo.TotalPageFile // 1024,
        "as": meminfo.AvailPageFile // 1024,
        "ml": meminfo.MemoryLoad,
    }

    pinfo = get_perf_info()
    data["prf"] = {
        "mc": pinfo.SystemCacheBytes,
        "pr": pinfo.ProcessCount,
        "ke": pinfo.KernelTotal,
    }

    counters = [
        r'\Processor(_Total)\% Processor Time',
        r'\PhysicalDisk(_Total)\% Disk Time',
        r'\PhysicalDisk(_Total)\Disk Read Bytes/sec',
        r'\PhysicalDisk(_Total)\Disk Write Bytes/sec',
        r'\PhysicalDisk(_Total)\Disk Reads/sec',
        r'\PhysicalDisk(_Total)\Disk Writes/sec',
        #        r'\Network Adapter(*)\Bytes Received/sec',
        #        r'\Network Adapter(*)\Bytes Sent/sec',
        #        r'\Network Adapter(*)\Packets Received/sec',
        #        r'\Network Adapter(*)\Packets Sent/sec',
    ]
    fmts = [
        "double",
        "double",
        "double",
        "double",
        "double",
        "double",
        #        "double",
        #        "double",
        #        "double",
        #        "double",
    ]
    mon = get_perf_data(counters, fmts=fmts, delay=2000, english=True)
    data["mon"] = {
        "pt": mon[0],
    }
    data["dev"] = {
        "tm": mon[1],
        "rb": mon[2],
        "wb": mon[3],
        "r": mon[2],
        "w": mon[3],
    }

    stats_d = os.path.join(Env.paths.pathvar, "stats")
    stats_p = os.path.join(stats_d, 'sa%d' % now.day)

    if not os.path.exists(stats_d):
        os.makedirs(stats_d)
    if not os.path.exists(stats_p):
        # create the stats file
        mode = 'w+'
    elif os.stat(stats_p).st_mtime < time.time() - 86400:
        # reset the stats file from last month
        mode = 'w+'
    else:
        # append to the daily stats file
        mode = 'a'

    with open(stats_p, mode) as fd:
        json.dump(data, fd)
        fd.write(os.linesep)


if __name__ == "__main__":
    collect()
