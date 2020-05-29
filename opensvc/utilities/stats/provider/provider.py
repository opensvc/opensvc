import datetime
import os

from env import Env
from utilities.converters import convert_datetime


class BaseStatsProvider(object):
    one_minute = datetime.timedelta(minutes=1)
    one_day = datetime.timedelta(days=1)

    def __init__(self, interval=2880, stats_dir=None, stats_start=None, stats_end=None):
        self.interval = interval
        self.stats_dir = stats_dir
        self.stats_start = None
        self.stats_end = None
        self.init_period(stats_start, stats_end, interval)

    def init_period(self, stats_start, stats_end, interval):
        if stats_end is None:
            self.stats_end = datetime.datetime.now()
        else:
            self.stats_end = convert_datetime(stats_end)

        if stats_start is None:
            self.stats_start = self.stats_end - datetime.timedelta(minutes=interval)
        else:
            self.stats_start = convert_datetime(stats_start)
            delta = self.stats_end - self.stats_start
            interval = delta.days * 1440 + delta.seconds // 60

        # discard seconds
        self.stats_start -= datetime.timedelta(seconds=self.stats_start.second)
        self.stats_end += datetime.timedelta(seconds=60 - self.stats_end.second)

    def get(self, stat_name):
        stat_name_provider = getattr(self, stat_name)
        if not stat_name_provider:
            print(stat_name, 'is not implemented')
            return [], []
        return self._stat_transformer(stat_name_provider)

    def _stat_transformer(self, stat_provider):
        return [], []


class BaseStatsProviderUx(BaseStatsProvider):
    def __init__(self, interval=2880, stats_dir=None, stats_start=None, stats_end=None):
        super(BaseStatsProviderUx, self).__init__(interval, stats_dir, stats_start, stats_end)
        self.nodename = Env.nodename

        self.minutes_first_day = 60 * self.stats_end.hour + self.stats_end.minute + 1

        self.ranges = []
        i = 0
        end = self.stats_end

        while end > self.stats_start:
            start = end - self.one_day
            if start < self.stats_start:
                start = self.stats_start
            if start.day != end.day:
                start = end - datetime.timedelta(hours=end.hour, minutes=end.minute)
            if start != end:
                self.ranges.append((start, end))
            end = start - self.one_minute
        # print(self.stats_end,
        #       interval,
        #       [x.strftime("%Y-%m-%d %H:%M:%S")+" - "+y.strftime("%Y-%m-%d %H:%M:%S") for x, y in self.ranges])

    def _stat_transformer(self, stat_provider):
        lines = []
        cols = []
        for start, end in self.ranges:
            date = start.strftime("%Y-%m-%d")
            day = start.strftime("%d")
            start = start.strftime("%H:%M:%S")
            end = end.strftime("%H:%M:%S")
            _cols, _lines = stat_provider(date, day, start, end)
            if len(_cols) == 0 or len(_lines) == 0:
                continue
            cols = _cols
            lines += _lines
        return cols, lines

    def sarfile(self, day):
        if self.stats_dir is None:
            stats_dir = os.path.join(os.sep, 'var', 'log', 'sysstat')
            if not os.path.exists(stats_dir):
                stats_dir = os.path.join(os.sep, 'var', 'log', 'sa')
        else:
            stats_dir = self.stats_dir
        f = os.path.join(stats_dir, 'sa' + day)
        if os.path.exists(f):
            return f
        return None

    def cpu(self, d, day, start, end):
        return [], []

    def mem_u(self, d, day, start, end):
        return [], []

    def proc(self, d, day, start, end):
        return [], []

    def swap(self, d, day, start, end):
        return [], []

    def block(self, d, day, start, end):
        return [], []

    def blockdev(self, d, day, start, end):
        return [], []

    def netdev(self, d, day, start, end):
        return [], []

    def netdev_err(self, d, day, start, end):
        return [], []


if __name__ == "__main__":
    sp = BaseStatsProviderUx(interval=20)
    print(sp.get('cpu'))
    print(sp.get('swap'))
