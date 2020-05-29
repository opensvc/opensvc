import datetime
import os

from utilities.proc import call
from utilities.stats.provider import provider


class StatsProvider(provider.BaseStatsProviderUx):
    def __init__(self, interval=2880, stats_dir=None, stats_start=None, stats_end=None):
        super(StatsProvider, self).__init__(interval, stats_dir, stats_start, stats_end)
        cmd = ['pagesize']
        (ret, pagesize, err) = call(cmd)
        self.pagesize = int(pagesize)

    def zsfile(self, day):
        f = os.path.join(os.sep, 'var', 'adm', 'zonestat', 'zs' + day)
        if os.path.exists(f):
            return f
        return None

    def sarfile(self, day):
        f = os.path.join(os.sep, 'var', 'adm', 'sa', 'sa' + day)
        if os.path.exists(f):
            return f
        return None

    def svc(self, d, day, start, end):
        cols = ['date',
                'svcname',
                'swap',
                'rss',
                'cap',
                'at',
                'avgat',
                'pg',
                'avgpg',
                'nproc',
                'mem',
                'cpu',
                'nodename']
        f = self.zsfile(day)
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
            if len(l) != 17:
                continue
            _d = datetime.datetime.strptime(" ".join(l[0:2]), "%Y-%m-%d %H:%M:%S")
            _d = _d.hour * 3600 + _d.minute * 60 + _d.second
            if _d < _start or _d > _end:
                continue
            for i, e in enumerate(l):
                if e.endswith('T'):
                    l[i] = str(int(e[0:-1]) * 1024 * 1024)
                elif e.endswith('G'):
                    l[i] = str(int(e[0:-1]) * 1024)
                elif e.endswith('M'):
                    l[i] = e.rstrip('M')
                elif e.endswith('K'):
                    l[i] = str(1.0 * int(e[0:-1]) / 1024)
            l = [" ".join(l[0:2])] + l[2:-4] + [self.nodename]
            lines.append(l)
        return cols, lines

    def cpu(self, d, day, start, end):
        cols = ['date',
                'usr',
                'sys',
                'iowait',
                'idle',
                'cpu',
                'nodename']
        f = self.sarfile(day)
        lines = []
        if f is None:
            return cols, lines
        cmd = ['sar', '-u', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd, errlog=False)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 5:
                continue
            if l[1] == '%usr':
                continue
            if l[0] == 'Average':
                continue
            l += ['all', self.nodename]
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def mem_u(self, d, day, start, end):
        cols = ['date',
                'kbmemfree',
                'nodename']
        f = self.sarfile(day)
        lines = []
        if f is None:
            return cols, lines
        cmd = ['sar', '-r', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 3:
                continue
            if l[1] == 'freemem':
                continue
            if l[0] == 'Average':
                continue

            try:
                freemem = int(l[1]) * self.pagesize / 1024
            except:
                continue
            x = ['%s %s' % (d, l[0]), str(freemem), self.nodename]
            lines.append(x)
        return cols, lines

    def proc(self, d, day, start, end):
        cols = ['date',
                'runq_sz',
                'nodename']
        f = self.sarfile(day)
        lines = []
        if f is None:
            return cols, lines
        cmd = ['sar', '-q', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 5:
                continue
            if l[1] == 'runq-sz':
                continue
            if l[0] == 'Average':
                continue
            x = ['%s %s' % (d, l[0]), l[1], self.nodename]
            lines.append(x)
        return cols, lines

    def swap(self, d, day, start, end):
        cols = ['date',
                'kbswpfree',
                'nodename']
        f = self.sarfile(day)
        lines = []
        if f is None:
            return cols, lines
        cmd = ['sar', '-r', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 3:
                continue
            if l[1] == 'freemem':
                continue
            if l[0] == 'Average':
                continue

            try:
                freeswap = int(l[2]) / 2
            except:
                continue
            x = ['%s %s' % (d, l[0]), str(freeswap), self.nodename]
            lines.append(x)
        return cols, lines

    def block(self, d, day, start, end):
        cols = ['date',
                'rbps',
                'wbps',
                'nodename']
        f = self.sarfile(day)
        lines = []
        if f is None:
            return [], []
        cmd = ['sar', '-b', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 9:
                continue
            if l[1] == 'bread/s':
                continue
            if l[0] == 'Average':
                continue
            x = ['%s %s' % (d, l[0]), l[1], l[4], self.nodename]
            lines.append(x)
        return cols, lines

    def blockdev(self, d, day, start, end):
        cols = ['date',
                'dev',
                'pct_util',
                'avgqu_sz',
                'rsecps',
                'await',
                'svctm',
                'nodename']
        f = self.sarfile(day)
        lines = []
        if f is None:
            return cols, lines
        cmd = ['sar', '-d', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd, errlog=False)
        last_date = '00:00:00'
        for line in buff.split('\n'):
            l = line.split()
            if len(l) == 8:
                last_date = l[0]
            if len(l) == 7:
                l = [last_date] + l
            if len(l) != 8:
                continue
            if l[1] == 'device':
                continue
            if l[0] == 'Average':
                continue
            # 00:00:00 device %busy avque r+w/s [blks/s] avwait avserv
            x = ['%s %s' % (d, l[0]), l[1], l[2], l[3], l[4], l[6], l[7], self.nodename]
            lines.append(x)
        return cols, lines
