#!/usr/bin/python2.6
#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>'
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
from rcUtilities import call, which
import rcStats

class StatsProvider(rcStats.StatsProvider):
    def __init__(self, collect_file=None, collect_date=None, interval=2880):
        rcStats.StatsProvider.__init__(self, collect_file=None,
                                       collect_date=None, interval=2880)
        cmd = ['pagesize']
        (ret, pagesize) = call(cmd)
        self.pagesize = int(pagesize)

    def sarfile(self, day):
        f = os.path.join(os.sep, 'var', 'adm', 'sa', 'sa'+day)
        if os.path.exists(f):
            return f
        return None

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
        cmd = ['sar', '-u', '-f', f]
        (ret, buff) = call(cmd, errlog=False)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 5:
                continue
            if l[1] == '%usr':
                continue
            if l[0] == 'Average':
                continue
            l += ['all', self.nodename]
            l[0] = '%s %s'%(d, l[0])
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
        cmd = ['sar', '-r', '-f', f]
        (ret, buff) = call(cmd)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 3:
                continue
            if l[1] == 'freemem':
                continue
            if l[0] == 'Average':
                continue

            freemem = int(l[1])*self.pagesize/1024
            x = ['%s %s'%(d, l[0]), str(freemem), self.nodename]
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
        cmd = ['sar', '-q', '-f', f]
        (ret, buff) = call(cmd)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 5:
                continue
            if l[1] == 'runq-sz':
                continue
            if l[0] == 'Average':
                continue
            x = ['%s %s'%(d, l[0]), l[1], self.nodename]
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
        cmd = ['sar', '-r', '-f', f]
        (ret, buff) = call(cmd)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 3:
                continue
            if l[1] == 'freemem':
                continue
            if l[0] == 'Average':
                continue

            freeswap = int(l[2])/2
            x = ['%s %s'%(d, l[0]), str(freeswap), self.nodename]
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
            return []
        cmd = ['sar', '-b', '-f', f]
        (ret, buff) = call(cmd)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 9:
                continue
            if l[1] == 'bread/s':
                continue
            if l[0] == 'Average':
                continue
            x = ['%s %s'%(d, l[0]), l[1], l[4], self.nodename]
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
        cmd = ['sar', '-d', '-f', f]
        (ret, buff) = call(cmd, errlog=False)
        for line in buff.split('\n'):
            l = line.split()
            if len(l) == 8:
                date = l[0]
            if len(l) == 7:
                l = [date] + l
            if len(l) != 8:
                continue
            if l[1] == 'device':
                continue
            if l[0] == 'Average':
                continue
            # 00:00:00 device %busy avque r+w/s [blks/s] avwait avserv
            x = ['%s %s'%(d, l[0]), l[1], l[2], l[3], l[4], l[6], l[7], self.nodename]
            lines.append(x)
        return cols, lines

