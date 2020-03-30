from __future__ import print_function

import datetime
import json
import os
import re
import time

from env import Env
from utilities.proc import justcall, which

mntpt_blacklist = [
    "/proc",
    "/sys/fs/cgroup",
    "/run/user/[0-9]+",
    "(/var){0,1}/run/user/[0-9]+",
    ".*/docker/.*/[0-9a-f]{64}.*",
]


def collect(node):
    now = str(datetime.datetime.now())

    def blacklisted(mntpt):
        for bl in mntpt_blacklist:
            if re.match(bl, mntpt):
                return True

    def fs_u():
        cmd = ['df', '-lP']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return
        lines = out.split('\n')
        if len(lines) < 2:
            return
        vals = []
        for line in lines[1:]:
            l = line.split()
            if len(l) != 6:
                continue
            if blacklisted(l[5]):
                continue
            vals.append([now, node.nodename, l[5], l[1], l[4].replace('%', '')])

        stats_fs_u_d = os.path.join(Env.paths.pathvar, "stats")
        stats_fs_u_p = os.path.join(stats_fs_u_d, 'fs_u.%d' % datetime.datetime.now().day)

        if not os.path.exists(stats_fs_u_d):
            os.makedirs(stats_fs_u_d)
        if not os.path.exists(stats_fs_u_p):
            # create the stats file
            mode = 'w+'
        elif os.stat(stats_fs_u_p).st_mtime < time.time() - 86400:
            # reset the stats file from last month
            mode = 'w+'
        else:
            # append to the daily stats file
            mode = 'a'

        with open(stats_fs_u_p, mode) as f:
            f.write(json.dumps(vals) + '\n')

    """
    xentop
    NAME STATE CPU(sec) CPU(%) MEM(k) MEM(%) MAXMEM(k) MAXMEM(%) VCPUS NETS NETTX(k) NETRX(k) VBDS VBD_OO VBD_RD VBD_WR VBD_RSECT VBD_WSECT SSID
    """

    def xentop(node):
        import os
        import time
        import datetime
        import subprocess

        if not which('xentop'):
            return

        node.build_services()
        containernames = {}
        for svc in node.svcs:
            for r in svc.get_resources("container"):
                if r.type in ("container.ovm", "container.xen"):
                    if hasattr(r, "uuid"):
                        containernames[r.uuid] = r.name

        zs_d = os.path.join(Env.paths.pathlog, 'xentop')
        zs_prefix = 'xentop'
        zs_f = os.path.join(zs_d, zs_prefix + datetime.datetime.now().strftime("%d"))
        datenow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        n = datetime.datetime.now()
        tn = time.mktime(n.timetuple())

        if not os.path.exists(zs_d):
            os.makedirs(zs_d)

        try:
            t = os.path.getmtime(zs_f)
            d = tn - t
        except:
            d = 0

        if d > 27 * 24 * 3600:
            os.remove(zs_f)

        f = open(zs_f, "a")

        stor = {}

        p = subprocess.Popen('xentop -b -d.1 -i2 -f',
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             shell=True,
                             bufsize=0)

        out = p.stdout.readline()
        pr = 0

        while out:
            line = out
            line = line.rstrip("\n")

            if "NAME" in line:
                pr += 1
                out = p.stdout.readline()
                continue

            line = line.replace("no limit", "0")
            fields = line.split()
            if len(fields) == 19 and pr > 1:
                uuid = fields[0]
                if uuid in containernames:
                    uuid = containernames[uuid]
                stor[uuid] = {
                    'STATE': fields[1],
                    'CPU_SEC': fields[2],
                    'CPU_PCT': fields[3],
                    'MEM': str(int(fields[4]) // 1024),
                    'MEM_PCT': fields[5],
                    'MEM_MAX': str(int(fields[6]) // 1024),
                    'MEM_MAX_PCT': fields[7],
                    'VCPUS': fields[8],
                    'NETS': fields[9],
                    'NET_TX': fields[10],
                    'NET_RX': fields[11],
                    'VBDS': fields[12],
                    'VBD_OO': fields[13],
                    'VBD_RD': fields[14],
                    'VBD_WR': fields[15],
                    'VBD_RSECT': fields[16],
                    'VBD_WSECT': fields[17],
                    'SSID': fields[18]
                }
                print(datenow, uuid, stor[uuid]['STATE'], stor[uuid]['CPU_SEC'], stor[uuid]['CPU_PCT'],
                      stor[uuid]['MEM'], stor[uuid]['MEM_PCT'], stor[uuid]['MEM_MAX'], stor[uuid]['MEM_MAX_PCT'],
                      stor[uuid]['VCPUS'], stor[uuid]['NETS'], stor[uuid]['NET_TX'], stor[uuid]['NET_RX'],
                      stor[uuid]['VBDS'], stor[uuid]['VBD_OO'], stor[uuid]['VBD_RD'], stor[uuid]['VBD_WR'],
                      stor[uuid]['VBD_RSECT'], stor[uuid]['VBD_WSECT'], stor[uuid]['SSID'], file=f)
            out = p.stdout.readline()

        p.wait()

    fs_u()
    xentop(node)
