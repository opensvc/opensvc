import os

from env import Env
from utilities.proc import call
from utilities.stats.provider import provider


class StatsProvider(provider.BaseStatsProviderUx):
    def customfile(self, metric, day):
        f = os.path.join(Env.paths.pathvar, 'stats', metric + day)
        if os.path.exists(f):
            return f
        return None

    def cpu(self, d, day, start, end):
        cols = ['date',
                'cpu',
                'usr',
                'nice',
                'sys',
                'idle',
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
            if l[0] == 'Average:':
                continue
            (time, usr, nice, sys, idle) = l
            l = ['%s %s' % (d, time), 'all', usr, nice, sys, idle, self.nodename]
            lines.append(l)
        return cols, lines

    def mem_u(self, d, day, start, end):
        cols = ['date',
                'kbmemfree',
                'kbmemused',
                'kbbuffers',
                'kbcached',
                'kbmemsys',
                'nodename']
        fname = self.customfile('mem_u', day)
        lines = []
        if fname is None:
            return cols, lines
        try:
            f = open(fname, 'r')
            buff = f.read()
            f.close()
        except:
            return cols, lines
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 6:
                continue
            (time, free, inactive, active, speculative, wired) = l
            l = ['%s %s' % (d, time), free, active, speculative, inactive, wired, self.nodename]
            lines.append(l)
        return cols, lines

    def blockdev(self, d, day, start, end):
        cols = ['date',
                'dev',
                'tps',
                'rsecps',
                'nodename']
        f = self.sarfile(day)
        lines = []
        if f is None:
            return cols, lines
        cmd = ['sar', '-d', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd, errlog=False)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 4:
                continue
            if l[1] == 'device':
                continue
            if l[1] == 'Disk:':
                continue
            if l[0] == 'Average:':
                continue
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def netdev(self, d, day, start, end):
        cols = ['date',
                'dev',
                'rxpckps',
                'rxkBps',
                'txpckps',
                'txkBps',
                'nodename']

        f = self.sarfile(day)
        lines = []
        if f is None:
            return cols, lines
        cmd = ['sar', '-n', 'DEV', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd, errlog=False)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 6:
                continue
            if l[1] in ['IFACE', 'lo0']:
                continue
            if 'dummy' in l[1] or 'vnet' in l[1] or 'veth' in l[1] or \
                    'gif' in l[1] or 'stf' in l[1]:
                continue
            if l[0] == 'Average:':
                continue
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def netdev_err(self, d, day, start, end):
        cols = ['date',
                'dev',
                'rxerrps',
                'txerrps',
                'collps',
                'rxdropps',
                'nodename']
        f = self.sarfile(day)
        lines = []
        if f is None:
            return cols, lines
        cmd = ['sar', '-n', 'EDEV', '-f', f, '-s', start, '-e', end]
        (ret, buff, err) = call(cmd, errlog=False)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 6:
                continue
            if l[1] in ['IFACE', 'lo0']:
                continue
            if 'dummy' in l[1] or 'vnet' in l[1] or 'veth' in l[1] or \
                    'gif' in l[1] or 'stf' in l[1]:
                continue
            if l[0] == 'Average:':
                continue
            l.append(self.nodename)
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines
