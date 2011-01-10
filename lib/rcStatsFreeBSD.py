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

def twodays(fn):
    if which('bsdsar') is None:
        return []
    lines = fn(yesterday)
    lines += fn(today)
    return lines

def stats_cpu(file, collect_date=None):
    return twodays(stats_cpu_day)

def stats_cpu_day(t):
    """
        ['date',
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
         'nodename'],
    """
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    cmd = ['bsdsar', '-u', '-n', day]
    (ret, buff) = call(cmd, errlog=False)
    if ret != 0:
        cmd = ['bsdsar', '-u', '-n', day]
        (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
        l = line.split()
        if len(l) != 6:
            continue
        if l[0] == 'Time':
            continue
        x = [l[0], 'ALL', l[1], l[3], l[2], '0', '0', l[4], '0', '0', l[5], rcEnv.nodename]
        x[0] = '%s %s'%(d, x[0])
        lines.append(x)
    return lines

def stats_mem_u(file, collect_date=None):
    return twodays(stats_mem_u_day)

def kb(s):
    n = int(s[0:-1])
    unit = s[-1]
    if unit == 'k' or unit =='K':
        return n
    elif unit == 'M':
        return n*1024
    elif unit == 'G':
        return n*1024*1024
    elif unit == 'T':
        return n*1024*1024*1204
    elif unit == 'P':
        return n*1024*1024*1204*1024

def stats_mem_u_day(t):
    """
        ['date',
         'kbmemfree',
         'kbmemused',
         'pct_memused',
         'kbbuffers',
         'kbcached',
         'kbcommit',
         'pct_commit',
         'kbmemsys',
         'nodename'],
    """
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")

    cmd = ['sysctl', 'hw.physmem']
    (ret, out) = call(cmd)
    physmem = int(out.split(': ')[1])/1024

    cmd = ['sysctl', 'hw.usermem']
    (ret, out) = call(cmd)
    usermem = int(out.split(': ')[1])/1024

    cmd = ['bsdsar', '-r', '-n', day]
    (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 7:
           continue
       if l[0] == 'Time':
           continue
       free = kb(l[1])
       used = kb(l[2])+kb(l[3])
       x = [l[0], str(free), str(used), str(used/(used+free)), '0', '0', '0', '0', str(physmem-usermem)]
       x.append(rcEnv.nodename)
       x[0] = '%s %s'%(d, x[0])
       lines.append(x)
    return lines

def stats_proc(file, collect_date=None):
    return twodays(stats_proc_day)

def stats_proc_day(t):
    return []

def stats_swap(file, collect_date=None):
    return twodays(stats_swap_day)

def stats_swap_day(t):
    """
        ['date',
         'kbswpfree',
         'kbswpused',
         'pct_swpused',
         'kbswpcad',
         'pct_swpcad',
         'nodename'],

    """
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    cmd = ['bsdsar', '-r', '-n', day]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 7:
           continue
       if l[0] == 'Time':
           continue
       free = kb(l[6])
       used = kb(l[5])
       x = [l[0], str(free), str(used), str(used/(free+used)), '0', '0']
       x.append(rcEnv.nodename)
       x[0] = '%s %s'%(d, x[0])
       lines.append(x)
    return lines

def stats_block(file, collect_date=None):
    return twodays(stats_block_day)

def stats_block_day(t):
    return []

def stats_blockdev(file, collect_date=None):
    return twodays(stats_blockdev_day)

def stats_blockdev_day(t):
    return []

def stats_netdev(file, collect_date=None):
    return twodays(stats_netdev_day)

def stats_netdev_day(t):
    """
        ['date',
         'dev',
         'rxpckps',
         'txpckps',
         'rxkBps',
         'txkBps',
         'nodename'],
    """
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    cmd = ['bsdsar', '-I', '-n', day]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 9:
           continue
       if l[0] == 'Time':
           continue
       x = [l[0], l[8], l[1], l[4], l[3], l[6]]
       x.append(rcEnv.nodename)
       x[0] = '%s %s'%(x, x[0])
       lines.append(x)
    return lines


def stats_netdev_err(file, collect_date=None):
    return twodays(stats_netdev_err_day)

def stats_netdev_err_day(t):
    """
        ['date',
         'dev',
         'rxerrps',
         'txerrps',
         'collps',
         'rxdropps',
         'txdropps',
         'nodename'],
    """
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    cmd = ['bsdsar', '-I', '-n', day]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 9:
           continue
       if l[0] == 'Time':
           continue
       x = [l[0], l[8], l[2], l[5], l[7], '0', '0']
       x.append(rcEnv.nodename)
       x[0] = '%s %s'%(x, l[0])
       lines.append(x)
    return lines


