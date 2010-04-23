#!/usr/bin/python2.6
#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
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
from rcUtilities import call, which
from rcGlobalEnv import rcEnv

today = datetime.datetime.today()
yesterday = today - datetime.timedelta(days=1)

def glancefile(day):
    f = os.path.join(rcEnv.pathvar, 'glance'+day)
    if os.path.exists(f):
        return f
    return None

def twodays(fn):
    lines = fn(yesterday)
    lines += fn(today)
    return lines

def stats_cpu():
    return twodays(stats_cpu_day)

def stats_cpu_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = glancefile(day)
    if f is None:
        return []
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
            ts = '%s %s'%(d, l[0])
            ts = ts.replace('\0','')
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
                 rcEnv.nodename]
            lines.append(x)
        return lines

def stats_mem_u():
    return twodays(stats_mem_u_day)

def stats_mem_u_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = glancefile(day)
    if f is None:
        return []
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
            pct_commit = 100 * commit / vm
            pct_used = 100 * used / phys

            ts = '%s %s'%(d, l[0])
            ts = ts.replace('\0','')
            x = [ts,
                 l[8],
                 str(used),
                 str(pct_used),
                 l[9],
                 l[10],
                 str(commit),
                 str(pct_commit),
                 l[11],
                 rcEnv.nodename]
            lines.append(x)
    return lines

def stats_proc():
    return twodays(stats_proc_day)

def stats_proc_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = glancefile(day)
    if f is None:
        return []
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
            ts = '%s %s'%(d, l[0])
            ts = ts.replace('\0','')
            x = [ts,
                 l[18],
                 l[19],
                 l[15],
                 l[16],
                 l[17],
                 rcEnv.nodename]
            lines.append(x)
    return lines

def stats_swap():
    return twodays(stats_swap_day)

def stats_swap_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = glancefile(day)
    if f is None:
        return []
    lines = []
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

            ts = '%s %s'%(d, l[0])
            ts = ts.replace('\0','')
            x = [ts,
                 l[13],
                 l[14],
                 str(100 * swapused / swap),
                 '0',
                 '0',
                 rcEnv.nodename]
            lines.append(x)
    return lines

def stats_block():
    return twodays(stats_block_day)

def stats_block_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = glancefile(day)
    if f is None:
        return []
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
            ts = '%s %s'%(d, l[0])
            ts = ts.replace('\0','')
            x = [ts,
                 str(tps),
                 l[20],
                 l[21],
                 l[22],
                 l[23],
                 rcEnv.nodename]
            lines.append(x)
    return lines


def stats_blockdev():
    return twodays(stats_blockdev_day)

def stats_blockdev_day(t):
    return []

