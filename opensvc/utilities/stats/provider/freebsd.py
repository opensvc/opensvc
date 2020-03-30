from utilities.proc import call
from utilities.stats.provider import provider


class StatsProvider(provider.BaseStatsProviderUx):
    def cpu(self, d, day, start, end):
        cols = ['date',
                'usr',
                'sys',
                'nice',
                'irq',
                'idle',
                'cpu',
                'nodename']
        cmd = ['bsdsar', '-u', '-n', day]
        (ret, buff, err) = call(cmd, errlog=False)
        lines = []
        if ret != 0:
            return cols, lines
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 6:
                continue
            if l[0] == 'Time':
                continue
            l += ['ALL', self.nodename]
            l[0] = '%s %s' % (d, l[0])
            lines.append(l)
        return cols, lines

    def kb(self, s):
        n = int(s[0:-1])
        unit = s[-1]
        if unit == 'k' or unit == 'K':
            return n
        elif unit == 'M':
            return n * 1024
        elif unit == 'G':
            return n * 1024 * 1024
        elif unit == 'T':
            return n * 1024 * 1024 * 1204
        elif unit == 'P':
            return n * 1024 * 1024 * 1204 * 1024

    def mem_u(self, d, day, start, end):
        cols = ['date',
                'kbmemfree',
                'kbmemused',
                'pct_memused',
                'kbmemsys',
                'nodename']

        cmd = ['sysctl', 'hw.physmem']
        (ret, out, err) = call(cmd)
        physmem = int(out.split(': ')[1]) / 1024

        cmd = ['sysctl', 'hw.usermem']
        (ret, out, err) = call(cmd)
        usermem = int(out.split(': ')[1]) / 1024

        cmd = ['bsdsar', '-r', '-n', day]
        (ret, buff, err) = call(cmd)
        lines = []
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 7:
                continue
            if l[0] == 'Time':
                continue
            free = self.kb(l[1])
            used = self.kb(l[2]) + self.kb(l[3])
            x = [l[0], str(free), str(used), str(used / (used + free)), str(physmem - usermem), self.nodename]
            x[0] = '%s %s' % (d, x[0])
            lines.append(x)
        return cols, lines

    def swap(self, d, day, start, end):
        cols = ['date',
                'kbswpfree',
                'kbswpused',
                'pct_swpused',
                'kbswpcad',
                'pct_swpcad',
                'nodename']
        cmd = ['bsdsar', '-r', '-n', day]
        (ret, buff, err) = call(cmd, errlog=False)
        lines = []
        if ret != 0:
            return cols, lines
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 7:
                continue
            if l[0] == 'Time':
                continue
            free = self.kb(l[6])
            used = self.kb(l[5])
            x = [l[0], str(free), str(used), str(used / (free + used)), '0', '0']
            x.append(self.nodename)
            x[0] = '%s %s' % (d, x[0])
            lines.append(x)
        return cols, lines

    def netdev(self, d, day, start, end):
        cols = ['date',
                'rxpckps',
                'rxkBps',
                'txpckps',
                'txkBps',
                'dev',
                'nodename']
        cmd = ['bsdsar', '-I', '-n', day]
        (ret, buff, err) = call(cmd, errlog=False)
        lines = []
        if ret != 0:
            return cols, lines
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 9:
                continue
            if l[0] == 'Time':
                continue
            x = [l[0], l[1], l[3], l[4], l[6], l[8], self.nodename]
            x[0] = '%s %s' % (d, x[0])
            lines.append(x)
        return cols, lines

    def netdev_err(self, d, day, start, end):
        cols = ['date',
                'rxerrps',
                'txerrps',
                'collps',
                'dev',
                'nodename']
        cmd = ['bsdsar', '-I', '-n', day]
        (ret, buff, err) = call(cmd, errlog=False)
        lines = []
        if ret != 0:
            return cols, lines
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 9:
                continue
            if l[0] == 'Time':
                continue
            x = [l[0], l[2], l[5], l[7], l[8], self.nodename]
            x[0] = '%s %s' % (d, l[0])
            lines.append(x)
        return cols, lines
