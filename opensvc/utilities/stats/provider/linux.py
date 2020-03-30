import datetime
import os

from env import Env
from utilities.proc import justcall
from utilities.stats.provider import provider


class StatsProvider(provider.BaseStatsProviderUx):
    def xentopfile(self, day):
        f = os.path.join(Env.paths.pathlog, 'xentop', 'xentop' + day)
        if os.path.exists(f):
            return f
        return None

    def svc(self, d, day, start, end):
        cols = ['date',
                'svcname',
                'cpu',
                'mem',
                'cap',
                'cap_cpu',
                'nodename']
        f = self.xentopfile(day)
        lines = []
        if f is None:
            return cols, lines
        try:
            with open(f, 'r') as f:
                buff = f.read()
        except:
            return cols, lines
        _start = datetime.datetime.strptime(start, "%H:%M:%S")
        _start = _start.hour * 3600 + _start.minute * 60 + _start.second
        _end = datetime.datetime.strptime(end, "%H:%M:%S")
        _end = _end.hour * 3600 + _end.minute * 60 + _end.second
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 21:
                continue
            _d = datetime.datetime.strptime(" ".join(l[0:2]), "%Y-%m-%d %H:%M:%S")
            _d = _d.hour * 3600 + _d.minute * 60 + _d.second
            if _d < _start or _d > _end:
                continue
            l = [" ".join((l[0], l[1]))] + [l[2], l[5], l[7], l[8], l[10], self.nodename]
            lines.append(l)
        return cols, lines

    def cpu(self, d, day, start, end):
        f = self.sarfile(day)
        if f is None:
            return [], []
        cmd = ['sar', '-t', '-u', 'ALL', '-P', 'ALL', '-f', f, '-s', start, '-e', end]
        (buff, err, ret) = justcall(cmd)
        if ret != 0:
            cmd = ['sar', '-t', '-u', '-P', 'ALL', '-f', f, '-s', start, '-e', end]
            (buff, err, ret) = justcall(cmd)
        cols = []
        lines = []
        for line in buff.split('\n'):
            l = line.split()
            if 'Linux' in l:
                continue
            if len(l) == 7:
                """ redhat 4
                    18:50:01 CPU %user %nice %system %iowait %idle
                """
                cols = ['date',
                        'cpu',
                        'usr',
                        'nice',
                        'sys',
                        'iowait',
                        'idle',
                        'nodename']
            elif len(l) == 8:
                """ redhat 5
                    05:20:01 CPU %user %nice %system %iowait %steal %idle
                """
                cols = ['date',
                        'cpu',
                        'usr',
                        'nice',
                        'sys',
                        'iowait',
                        'steal',
                        'idle',
                        'nodename']
            elif len(l) == 11:
                cols = ['date',
                        'cpu',
                        'usr',
                        'nice',
                        'sys',
                        'iowait',
                        'steal',
                        'irq',
                        'soft',
                        'guest',
                        'idle',
                        'nodename']
            elif len(l) == 12:
                cols = ['date',
                        'cpu',
                        'usr',
                        'nice',
                        'sys',
                        'iowait',
                        'steal',
                        'irq',
                        'soft',
                        'guest',
                        'gnice',
                        'idle',
                        'nodename']
            else:
                continue
            if l[1] == 'CPU':
                continue
            if l[0] == 'Average:':
                continue
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def mem_u(self, d, day, start, end):
        f = self.sarfile(day)
        if f is None:
            return [], []
        cmd = ['sar', '-t', '-r', '-f', f, '-s', start, '-e', end]
        buff, err, ret = justcall(cmd)

        if "kbavail" in buff:
            fmt = 5
            cols = ['date',
                    'kbmemfree',
                    'kbavail',
                    'kbmemused',
                    'pct_memused',
                    'kbbuffers',
                    'kbcached',
                    'kbcommit',
                    'pct_commit',
                    'kbactive',
                    'kbinact',
                    'kbdirty',
                    'nodename']
        elif "kbdirty" in buff:
            fmt = 4
            cols = ['date',
                    'kbmemfree',
                    'kbmemused',
                    'pct_memused',
                    'kbbuffers',
                    'kbcached',
                    'kbcommit',
                    'pct_commit',
                    'kbactive',
                    'kbinact',
                    'kbdirty',
                    'nodename']
        elif "kbactive" in buff:
            fmt = 3
            cols = ['date',
                    'kbmemfree',
                    'kbmemused',
                    'pct_memused',
                    'kbbuffers',
                    'kbcached',
                    'kbcommit',
                    'pct_commit',
                    'kbactive',
                    'kbinact',
                    'nodename']
        elif "pct_commit" in buff:
            fmt = 2
            cols = ['date',
                    'kbmemfree',
                    'kbmemused',
                    'pct_memused',
                    'kbbuffers',
                    'kbcached',
                    'kbcommit',
                    'pct_commit',
                    'nodename']
        else:
            fmt = 1
            cols = ['date',
                    'kbmemfree',
                    'kbmemused',
                    'pct_memused',
                    'kbbuffers',
                    'kbcached',
                    'nodename']

        n = len(cols) - 1
        lines = []
        for line in buff.splitlines():
            l = line.split()
            if fmt > 1:
                if len(l) != n:
                    continue
            else:
                if len(l) < n:
                    continue
                l = l[:n]
            if l[1] == 'kbmemfree':
                continue
            if l[0] == 'Average:':
                continue

            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def fs_u(self, d, day, start, end):
        now = datetime.datetime.now()
        _start = datetime.datetime.strptime(start, "%H:%M:%S")
        _start = _start.hour * 3600 + _start.minute * 60 + _start.second
        _end = datetime.datetime.strptime(end, "%H:%M:%S")
        _end = _end.hour * 3600 + _end.minute * 60 + _end.second
        d = os.path.join(Env.paths.pathvar, 'stats')
        f = os.path.join(d, 'fs_u.%s' % day.lstrip("0"))
        cols = ['date',
                'nodename',
                'mntpt',
                'size',
                'used']

        if not os.path.exists(d):
            return [], []
        if not os.path.exists(f):
            return [], []

        with open(f, 'r') as fd:
            buff = fd.read()

        import json
        lines = []
        for line in buff.split('\n'):
            try:
                l = json.loads(line)
            except:
                continue
            for _l in l:
                if len(_l) != 5:
                    continue
                _now = datetime.datetime.strptime(_l[0], "%Y-%m-%d %H:%M:%S.%f")
                _now = _now.hour * 3600 + _now.minute * 60 + _now.second
                if _now < _start or _now > _end:
                    continue
                lines.append(_l)

        return cols, lines

    def proc(self, d, day, start, end):
        f = self.sarfile(day)
        cols = ['date',
                'runq_sz',
                'plist_sz',
                'ldavg_1',
                'ldavg_5',
                'ldavg_15',
                'nodename']

        if f is None:
            return [], []
        cmd = ['sar', '-t', '-q', '-f', f, '-s', start, '-e', end]
        (buff, err, ret) = justcall(cmd)
        lines = []
        if "blocked" in buff:
            n_fields = 7
            drop_blocked = True
        else:
            n_fields = 6
            drop_blocked = False
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != n_fields:
                continue
            if l[1] == 'runq-sz':
                continue
            if l[0] == 'Average:':
                continue
            if drop_blocked:
                l = l[:-1]
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def swap(self, d, day, start, end):
        f = self.sarfile(day)
        cols = ['date',
                'kbswpfree',
                'kbswpused',
                'pct_swpused',
                'kbswpcad',
                'pct_swpcad',
                'nodename']

        if f is None:
            return [], []
        cmd = ['sar', '-t', '-S', '-f', f, '-s', start, '-e', end]
        (buff, err, ret) = justcall(cmd)
        if ret != 0:
            """ redhat 5
            """
            cmd = ['sar', '-t', '-r', '-f', f, '-s', start, '-e', end]
            (buff, err, ret) = justcall(cmd)
        lines = []
        for line in buff.split('\n'):
            l = line.split()
            if len(l) == 10:
                """ redhat 5
                """
                l = [l[0]] + l[6:] + ['0']
            if len(l) != 6:
                continue
            if 'kbswpfree' in l:
                continue
            if l[0] == 'Average:':
                continue
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def block(self, d, day, start, end):
        f = self.sarfile(day)
        cols = ['date',
                'tps',
                'rtps',
                'wtps',
                'rbps',
                'wbps',
                'nodename']

        if f is None:
            return [], []
        cmd = ['sar', '-t', '-b', '-f', f, '-s', start, '-e', end]
        (buff, err, ret) = justcall(cmd)
        lines = []
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 6:
                continue
            if l[1] == 'tps':
                continue
            if l[0] == 'Average:':
                continue
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def blockdev(self, d, day, start, end):
        f = self.sarfile(day)
        cols = ['date',
                'dev',
                'tps',
                'rsecps',
                'wsecps',
                'avgrq_sz',
                'avgqu_sz',
                'await',
                'svctm',
                'pct_util',
                'nodename']
        if f is None:
            return [], []
        cmd = ['sar', '-t', '-d', '-p', '-f', f, '-s', start, '-e', end]
        (buff, err, ret) = justcall(cmd)
        lines = []
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 10:
                continue
            if l[1] == 'DEV':
                continue
            if l[0] == 'Average:':
                continue
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def netdev(self, d, day, start, end):
        f = self.sarfile(day)
        cols = ['date',
                'dev',
                'rxpckps',
                'txpckps',
                'rxkBps',
                'txkBps',
                'nodename']

        if f is None:
            return [], []

        cmd = ['sar', '-t', '-n', 'DEV', '-f', f, '-s', start, '-e', end]
        (buff, err, ret) = justcall(cmd)

        if "%ifutil" in buff:
            n = 10
        else:
            n = 9

        lines = []
        div = 1
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != n:
                continue
            if l[1] in ['IFACE', 'lo']:
                if 'rxbyt/s' in l:
                    div = 1024
                continue
            if 'dummy' in l[1] or 'vnet' in l[1] or 'veth' in l[1] or \
                    'pan' in l[1] or 'sit' in l[1]:
                continue
            if l[0] == 'Average:':
                continue
            m = []
            m.append('%s %s' % (d, l[0]))
            m.append(l[1])
            m.append(str(float(l[4]) / div))
            m.append(str(float(l[5]) / div))
            m.append(l[2])
            m.append(l[3])
            m.append(self.nodename)
            lines.append(m)
        return cols, lines

    def netdev_err(self, d, day, start, end):
        f = self.sarfile(day)
        cols = ['date',
                'dev',
                'rxerrps',
                'txerrps',
                'collps',
                'rxdropps',
                'txdropps',
                'nodename']

        if f is None:
            return [], []
        cmd = ['sar', '-t', '-n', 'EDEV', '-f', f, '-s', start, '-e', end]
        (buff, err, ret) = justcall(cmd)
        lines = []
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 11:
                continue
            if l[1] in ['IFACE', 'lo']:
                continue
            if 'dummy' in l[1] or 'vnet' in l[1] or 'veth' in l[1] or \
                    'pan' in l[1] or 'sit' in l[1]:
                continue
            if l[0] == 'Average:':
                continue
            l = l[0:7]
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines


if __name__ == "__main__":
    sp = StatsProvider(interval=200)
    print(sp.get('mem_u'))
