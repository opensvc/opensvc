import os

from env import Env
from utilities.stats.provider import provider


class StatsProvider(provider.BaseStatsProviderUx):
    def glancefile(self, day):
        f = os.path.join(Env.paths.pathvar, 'stats', 'glance' + day)
        if os.path.exists(f):
            return f
        return None

    def cpu(self, d, day, start, end):
        f = self.glancefile(day)
        if f is None:
            return [], []
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
        lines = []
        with open(f, 'r') as file:
            for line in file:
                l = line.split()
                if len(l) != 24:
                    continue
                """ hpux:            usr nice sys irq wait idle
                                     1   2    3   4   5    6
                    xmlrpc: date cpu usr nice sys iowait steal irq soft guest idle nodename
                """
                ts = '%s %s' % (d, l[0])
                ts = ts.replace('\0', '')
                x = [ts,
                     'all',
                     l[1],
                     l[2],
                     l[3],
                     l[5],
                     '0',
                     l[4],
                     '0',
                     '0',
                     l[6],
                     self.nodename]
                lines.append(x)
            return cols, lines

    def mem_u(self, d, day, start, end):
        f = self.glancefile(day)
        if f is None:
            return [], []
        cols = ['date',
                'kbmemfree',
                'kbmemused',
                'pct_memused',
                'kbbuffers',
                'kbcached',
                'kbcommit',
                'pct_commit',
                'kbmemsys',
                'nodename']
        lines = []
        with open(f, 'r') as file:
            for line in file:
                l = line.split()
                if len(l) != 24:
                    continue
                """ hpux:            phys kbmemfree kbcached kbfilecached kbsys kbuser kbswapused kbswap
                                     7    8         9        10           11    12     13         14
                    xmlrpc: date kbmemfree kbmemused pct_memused kbbuffers kbcached kbcommit pct_commit kbmemsys nodename
                """
                phys = int(l[7])
                free = int(l[8])
                swapused = int(l[13])
                swap = int(l[14])
                used = phys - free
                commit = used + swapused
                vm = phys + swap
                if vm == 0 or phys == 0:
                    continue
                pct_commit = 100 * commit / vm
                pct_used = 100 * used / phys

                ts = '%s %s' % (d, l[0])
                ts = ts.replace('\0', '')
                x = [ts,
                     l[8],
                     str(used),
                     str(pct_used),
                     l[9],
                     l[10],
                     str(commit),
                     str(pct_commit),
                     l[11],
                     self.nodename]
                lines.append(x)
        return cols, lines

    def proc(self, d, day, start, end):
        f = self.glancefile(day)
        if f is None:
            return [], []
        cols = ['date',
                'runq_sz',
                'plist_sz',
                'ldavg_1',
                'ldavg_5',
                'ldavg_15',
                'nodename']
        lines = []
        with open(f, 'r') as file:
            for line in file.readlines():
                l = line.split()
                if len(l) != 24:
                    continue
                """ hpux:            GBL_LOADAVG GBL_LOADAVG5 GBL_LOADAVG15 GBL_CPU_QUEUE TBL_PROC_TABLE_USED
                                     15          16           17            18            19
                    xmlrpc: date runq_sz plist_sz ldavg_1 ldavg_5 ldavg_15 nodename
                """
                ts = '%s %s' % (d, l[0])
                ts = ts.replace('\0', '')
                x = [ts,
                     l[18],
                     l[19],
                     l[15],
                     l[16],
                     l[17],
                     self.nodename]
                lines.append(x)
        return cols, lines

    def swap(self, d, day, start, end):
        f = self.glancefile(day)
        if f is None:
            return [], []
        lines = []
        cols = ['date',
                'kbswpfree',
                'kbswpused',
                'pct_swpused',
                'kbswpcad',
                'pct_swpcad',
                'nodename']

        with open(f, 'r') as file:
            for line in file.readlines():
                l = line.split()
                if len(l) != 24:
                    continue
                """ hpux:        kbswapused kbswap
                                 13         14
                    xmlrpc: date kbswpfree kbswpused pct_swpused kbswpcad pct_swpcad nodename
                """
                swapused = int(l[13])
                swap = int(l[14])
                swapfree = swap - swapused

                ts = '%s %s' % (d, l[0])
                ts = ts.replace('\0', '')
                x = [ts,
                     str(swapfree),
                     l[13],
                     str(100 * swapused / swap),
                     '0',
                     '0',
                     self.nodename]
                lines.append(x)
        return cols, lines

    def block(self, d, day, start, end):
        f = self.glancefile(day)
        if f is None:
            return [], []
        cols = ['date',
                'tps',
                'rtps',
                'wtps',
                'rbps',
                'wbps',
                'nodename']

        lines = []
        with open(f, 'r') as file:
            for line in file.readlines():
                l = line.split()
                if len(l) != 24:
                    continue
                """ hpux:        rio wio rkb wkb
                                 20  21  22  23
                    xmlrpc: date tps rtps wtps rbps wbps nodename
                """
                tps = float(l[20]) + float(l[21])
                ts = '%s %s' % (d, l[0])
                ts = ts.replace('\0', '')
                x = [ts,
                     str(tps),
                     l[20],
                     l[21],
                     l[22],
                     l[23],
                     self.nodename]
                lines.append(x)
        return cols, lines
