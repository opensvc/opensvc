"""
YYYY-MM-DD hh:mm:ss ZONE SWAP RSS CAP at avgat pg avgpg NPROC mem% cpu% TIME LastReboot
"""

from __future__ import print_function

import datetime
import os
import platform
import subprocess
import time

from env import Env
from utilities.converters import convert_size
from utilities.proc import justcall, which


def collect(node):
    now = str(datetime.datetime.now())

    zs_d = os.path.join(os.sep, 'var', 'adm', 'zonestat')
    zs_prefix = 'zs'
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

    p = subprocess.Popen('/usr/bin/prstat -Z -n1,60 1 1',
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,
                         shell=True,
                         bufsize=0)

    out = p.stdout.readline()
    pr = 0

    while out:
        line = out
        line = line.rstrip("\n")

        if "ZONEID" in line:
            pr = 1
            out = p.stdout.readline()
            continue

        if "Total:" in line:
            pr = 0
            out = p.stdout.readline()
            continue

        if "%" in line and pr == 1:
            fields = line.split()
            stor[fields[7]] = {
                'SWAP': fields[2],
                'RSS': fields[3],
                'CAP': '0',
                'at': '0',
                'avgat': '0',
                'pg': '0',
                'avgpg': '0',
                'NPROC': fields[1],
                'mem': fields[4],
                'cpu': fields[6],
                'TIME': fields[5]
            }
        out = p.stdout.readline()

    p.wait()
    fi = 1
    pr = 0
    p = subprocess.Popen('/usr/bin/rcapstat -z 1 2',
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,
                         bufsize=0,
                         shell=True)
    out = p.stdout.readline()

    while out:
        line = out
        line = line.rstrip("\n")

        if "id zone" in line and fi == 1:
            fi = 0
            out = p.stdout.readline()
            continue

        if "id zone" in line and fi == 0:
            pr = 1
            out = p.stdout.readline()
            continue

        if "id zone" not in line and pr == 1:
            fields = line.split()
            stor[fields[1]]['CAP'] = fields[5]
            stor[fields[1]]['at'] = fields[6]
            stor[fields[1]]['avgat'] = fields[7]
            stor[fields[1]]['pg'] = fields[8]
            stor[fields[1]]['avgpg'] = fields[9]

        out = p.stdout.readline()

    p.wait()

    for z in stor:
        zn = z
        if z == 'global':
            zn = platform.node()
            p = subprocess.Popen('/usr/bin/who -b',
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 bufsize=0,
                                 shell=True)
        else:
            p = subprocess.Popen('/usr/sbin/zlogin ' + z + ' /usr/bin/who -b',
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 bufsize=0,
                                 shell=True)
        out = p.stdout.readline()
        txt = out.split()
        print(datenow, zn, stor[z]['SWAP'], stor[z]['RSS'], stor[z]['CAP'], stor[z]['at'], stor[z]['avgat'],
              stor[z]['pg'], stor[z]['avgpg'], stor[z]['NPROC'], stor[z]['mem'], stor[z]['cpu'], stor[z]['TIME'],
              txt[-3], txt[-2], txt[-1], file=f)
        p.wait()

    """
     fs_u
    """

    def fs_u():
        vars = ['date',
                'nodename',
                'mntpt',
                'size',
                'used']
        vals = []
        vals += fs_u_t("vxfs")
        vals += fs_u_t("ufs")
        vals += fs_u_zfs()
        return vars, vals

    def fs_u_t(t):
        if not which('df'):
            return []
        cmd = ['df', '-F', t, '-k']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return []
        lines = out.split('\n')
        if len(lines) < 2:
            return []
        vals = []
        for line in lines[1:]:
            l = line.split()
            if len(l) == 5:
                l = [''] + l
            elif len(l) != 6:
                continue
            vals.append([now, node.nodename, l[5], l[1], l[4].replace('%', '')])
        return vals

    def fs_u_zfs():
        if not which(Env.syspaths.zfs):
            return []
        cmd = [Env.syspaths.zfs, 'list', '-o', 'name,used,avail,mountpoint', '-H']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return []
        lines = out.split('\n')
        if len(lines) == 0:
            return []
        vals = []
        for line in lines:
            l = line.split()
            if len(l) != 4:
                continue
            if "@" in l[0]:
                # do not report clone usage
                continue
            if "osvc_sync_" in l[0]:
                # do not report osvc sync snapshots fs usage
                continue
            used = convert_size(l[1], _to="KB")
            if l[2] == '0':
                l[2] = '0K'
            avail = convert_size(l[2], _to="KB")
            total = used + avail
            pct = used / total * 100
            vals.append([now, node.nodename, l[0], str(total), str(pct)])
        return vals

    node.collector.call('push_stats_fs_u', fs_u())
