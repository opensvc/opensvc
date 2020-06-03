import json
import os

from env import Env
from utilities.converters import convert_datetime
from utilities.stats.provider import provider


class StatsProvider(provider.BaseStatsProvider):
    """
    {
        "ts": "2018-10-18 15:38:01.921000",
        "mem": {
            "tp": 2096748,
            "ap": 275536,
            "ts": 5304664,
            "as": 3027048,
            "ml": 86
        },
        "prf": {
            "pr": 98,
            "ke": 53234,
            "mc": 298704896
        },
        "dev": {
            "w": 32456.789373365064,
            "r": 0.0,
            "tm": 0.003836151746131958,
            "wb": 32456.789373365064,
            "rb": 0.0
        },
        "mon": {
            "pt": 3.0798430885735417
        }
    }                                                                                                                                                           
    """
    def __init__(self, interval=2880, stats_dir=None, stats_start=None, stats_end=None):
        super(StatsProvider, self).__init__(interval, stats_dir, stats_start, stats_end)
        self.data = self.get_data(self.stats_start, self.stats_end)

    def _stat_transformer(self, stat_provider):
        return stat_provider()

    def sarfile(self, day):
        f = os.path.join(Env.paths.pathvar, 'stats', 'sa%s' % day)
        if os.path.exists(f):
            return f
        return None

    def get_data(self, start, end):
        def get(day):
            _data = []
            sa = self.sarfile(day)
            if not sa:
                return []
            with open(sa, "r") as fd:
                for line in fd.readlines():
                    try:
                        __data = json.loads(line)
                    except ValueError:
                        continue
                    ts = convert_datetime(__data["ts"])
                    if ts < start or ts > end:
                        continue
                    _data.append(__data)
            return _data

        data = get(start.day)
        while start.day < end.day:
            start += self.one_day
            data += get(start.day)
        return data

    def cpu(self):
        if self.data is None:
            return [], []
        cols = ['date',
                'cpu',
                'usr',
                #                'nice',
                'sys',
                #                'iowait',
                #                'steal',
                'irq',
                #                'soft',
                #                'guest',
                #                'gnice',
                'idle',
                'nodename']
        lines = []
        for data in self.data:
            mon = data.get("mon", {})
            try:
                usr = mon["pt"]
                idle = 100 - usr
            except KeyError:
                continue
            lines.append([
                data["ts"],
                "all",
                usr,
                0,
                0,
                idle,
                Env.nodename
            ])
        return cols, lines

    def mem_u(self):
        if self.data is None:
            return [], []
        cols = ['date',
                'kbmemfree',
                #                'kbavail',
                'kbmemused',
                'pct_memused',
                #                'kbbuffers',
                #                'kbcached',
                #                'kbcommit',
                #                'pct_commit',
                #                'kbactive',
                #                'kbinact',
                #                'kbdirty',
                'nodename']
        lines = []
        for data in self.data:
            mem = data.get("mem", {})
            try:
                kbmemfree = mem["ap"]
                kbmemused = mem["tp"] - kbmemfree
                pct_memused = 100 * kbmemused // mem["tp"]
            except KeyError:
                continue
            lines.append([
                data["ts"],
                kbmemfree,
                kbmemused,
                pct_memused,
                Env.nodename
            ])
        return cols, lines

    def fs_u(self):
        if self.data is None:
            return [], []
        cols = ['date',
                'nodename',
                'mntpt',
                'size',
                'used']
        return [], []

    def proc(self):
        if self.data is None:
            return [], []
        cols = ['date',
                #                'runq_sz',
                'plist_sz',
                #                'ldavg_1',
                #                'ldavg_5',
                #                'ldavg_15',
                'nodename']
        lines = []
        for data in self.data:
            prf = data.get("prf", {})
            try:
                plist_sz = prf["pr"]
            except KeyError:
                continue
            lines.append([
                data["ts"],
                plist_sz,
                Env.nodename
            ])
        return cols, lines

    def swap(self):
        if self.data is None:
            return [], []
        cols = ['date',
                'kbswpfree',
                'kbswpused',
                'pct_swpused',
                #                'kbswpcad',
                #                'pct_swpcad',
                'nodename']
        lines = []
        for data in self.data:
            mem = data.get("mem", {})
            try:
                kbswpfree = mem["as"]
                kbswpused = mem["ts"] - kbswpfree
                pct_swpused = 100 * kbswpused // mem["ts"]
            except KeyError:
                continue
            lines.append([
                data["ts"],
                kbswpfree,
                kbswpused,
                pct_swpused,
                Env.nodename
            ])
        return cols, lines

    def block(self):
        if self.data is None:
            return [], []
        cols = ['date',
                'tps',
                'rtps',
                'wtps',
                'rbps',
                'wbps',
                'nodename']
        lines = []
        for data in self.data:
            dev = data.get("dev", {})
            try:
                rtps = dev["r"]
                wtps = dev["w"]
                tps = rtps + wtps
                rbps = dev["rb"]
                wbps = dev["wb"]
            except KeyError:
                continue
            lines.append([
                data["ts"],
                tps,
                rtps,
                wtps,
                rbps,
                wbps,
                Env.nodename
            ])
        return cols, lines

    def blockdev(self):
        if self.data is None:
            return [], []
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
        lines = []
        return cols, lines

    def netdev(self):
        if self.data is None:
            return [], []
        cols = ['date',
                'dev',
                'rxpckps',
                'txpckps',
                'rxkBps',
                'txkBps',
                'nodename']
        lines = []
        return cols, lines

    def netdev_err(self):
        if self.data is None:
            return [], []
        cols = ['date',
                'dev',
                'rxerrps',
                'txerrps',
                'collps',
                'rxdropps',
                'txdropps',
                'nodename']
        lines = []
        return cols, lines

    def svc(self):
        if self.data is None:
            return [], []
        cols = ['date',
                'svcname',
                'cpu',
                'mem',
                'cap',
                'cap_cpu',
                'nodename']
        lines = []
        return cols, lines


if __name__ == "__main__":
    sp = StatsProvider(interval=200)
    print(sp.get('mem_u'))
