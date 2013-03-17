#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#

import os
import datetime

class StatsProvider(object):
    def __init__(self, interval=2880, stats_dir=None, stats_start=None, stats_end=None):
        self.stats_dir = stats_dir
        self.interval = interval

        if stats_end is None:
            self.stats_end = datetime.datetime.now()
        else:
            self.stats_end = datetime.datetime.strptime(stats_end,"%Y-%m-%d %H:%M")

        if stats_start is None:
            self.stats_start = self.stats_end - datetime.timedelta(minutes=interval)
        else:
            self.stats_start = datetime.datetime.strptime(stats_start,"%Y-%m-%d %H:%M")
            delta = self.stats_end - self.stats_start
            interval = delta.days * 1440 + delta.seconds // 60

        x, self.nodename, x, x, x = os.uname()

        self.minutes_first_day = 60*self.stats_end.hour + self.stats_end.minute + 1

        one_minute = datetime.timedelta(minutes=1)
        one_day = datetime.timedelta(days=1)
        self.ranges = []
        i = 0
        end = self.stats_end

        while end > self.stats_start:
            start = end - one_day
            if start < self.stats_start:
                start = self.stats_start
            if start.day != end.day:
                start = end - datetime.timedelta(hours=end.hour, minutes=end.minute)
            if start != end:
                self.ranges.append((start, end))
            end = start - one_minute
        #print(self.stats_end, interval, map(lambda x: map(lambda y: y.strftime("%d-%m-%y %H:%M"), x), self.ranges))

    def get(self, fname):
        lines = []
        cols = []
        if not hasattr(self, fname):
            print(fname, 'is not implemented')
            return cols, lines
        for start, end in self.ranges:
            date = start.strftime("%Y-%m-%d")
            day = start.strftime("%d")
            start = start.strftime("%H:%M:%S")
            end = end.strftime("%H:%M:%S")
            _cols, _lines = getattr(self, fname)(date, day, start, end)
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
        f = os.path.join(stats_dir, 'sa'+day)
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
    sp = StatsProvider(interval=20)
    print(sp.get('cpu'))
    print(sp.get('swap'))
