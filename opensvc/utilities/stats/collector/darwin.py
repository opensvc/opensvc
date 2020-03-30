import datetime
import os

from env import Env
from utilities.proc import justcall


def collect(node):
    now = datetime.datetime.now()

    def fs_u():
        vars = ['date',
                'nodename',
                'mntpt',
                'size',
                'used']

        cmd = ['df', '-lkP']
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
            if l[5].startswith('/Volumes'):
                # Darwin automount package files under /Volumes
                continue
            vals.append([str(now), node.nodename, l[5], l[1], l[4].replace('%', '')])
        return vars, vals

    def mem_u():
        basedir = os.path.join(Env.paths.pathvar, 'stats')
        if not os.path.exists(basedir):
            os.makedirs(basedir)
        fname = os.path.join(basedir, 'mem_u%0.2d' % now.day)
        if not os.path.exists(fname):
            try:
                f = open(fname, 'w')
            except:
                return
        else:
            mtime = os.stat(fname).st_mtime
            if datetime.datetime.fromtimestamp(mtime) < now - datetime.timedelta(days=1):
                os.unlink(fname)
                try:
                    f = open(fname, 'w')
                except:
                    return
            else:
                try:
                    f = open(fname, 'a')
                except:
                    return

        cmd = ['/usr/sbin/sysctl', '-n', 'hw.pagesize']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return
        pagesize = int(out.split()[0])

        cmd = ['vm_stat']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return
        h = {}
        for line in out.split('\n'):
            l = line.split(':')
            if len(l) != 2:
                continue
            key = l[0]
            try:
                val = int(l[1].strip(' .'))
            except:
                continue
            h[key] = val
        f.write(' '.join((now.strftime('%H:%M:%S'),
                          str(h['Pages free'] * pagesize / 1024),
                          str(h['Pages active'] * pagesize / 1024),
                          str(h['Pages inactive'] * pagesize / 1024),
                          str(h['Pages speculative'] * pagesize / 1024),
                          str(h['Pages wired down'] * pagesize / 1024)
                          )) + '\n')

    data = fs_u()
    if data:
        node.collector.call('push_stats_fs_u', data)
    mem_u()
