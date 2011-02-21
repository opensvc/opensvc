#!/usr/bin/python2.6
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
    def __init__(self, collect_file=None, collect_date=None, interval=2880):
        self.collect_date = collect_date
        self.collect_file = collect_file
        self.interval = interval

        x, self.nodename, x, x, x = os.uname()
        self.now = datetime.datetime.now()
        self.today = datetime.datetime.today()
        self.yesterday = self.today - datetime.timedelta(days=1)

        self.minutes_today = 60*self.now.hour + self.now.minute

        self.ranges = []
        i = 0
        end = self.now
        while i < interval:
            if i == 0:
                if interval <= self.minutes_today:
                    delta = interval
                else:
                    delta = self.minutes_today
            elif i + 1440 > interval:
                delta = interval - i
            else:
                delta = 1440
            i += delta
            begin = end - datetime.timedelta(minutes=delta)
            self.ranges.append((begin, end))
            end = end - (end - begin)
        #print map(lambda x: map(lambda y: y.strftime("%d-%m-%y %H:%M"), x), self.ranges)

    def get(self, fname):
        lines = []
        cols = []
        for i, r in enumerate(self.ranges):
            t = self.today - datetime.timedelta(days=i)
            date = t.strftime("%Y-%m-%d")
            day = t.strftime("%d")
            start = r[0].strftime("%H:%M:%S")
            end = r[1].strftime("%H:%M:%S")
            _cols, _lines = getattr(self, fname)(date, day, start, end)
            if len(_cols) == 0 or len(_lines) == 0:
                continue
            cols = _cols
            lines += _lines
        return cols, lines

    def sarfile(self, day):
        f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
        if os.path.exists(f):
            return f
        f = os.path.join(os.sep, 'var', 'log', 'sa', 'sa'+day)
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
    print sp.get('cpu')
    print sp.get('swap')
