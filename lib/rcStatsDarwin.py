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

def customfile(metric, day):
    f = os.path.join(rcEnv.pathvar, 'stats', metric+day)
    if os.path.exists(f):
        return f
    return None

def sarfile(day):
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if os.path.exists(f):
        return f
    f = os.path.join(os.sep, 'var', 'log', 'sa', 'sa'+day)
    if os.path.exists(f):
        return f
    return None

def twodays(fn):
    if which('sar') is None:
        return []
    lines = fn(yesterday)
    lines += fn(today)
    return lines

def stats_cpu(file, collect_date=None):
    return twodays(stats_cpu_day)

def stats_cpu_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-u', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    for line in buff.split('\n'):
        l = line.split()
        if len(l) != 5:
            continue
        if l[1] == '%usr':
            continue
        if l[0] == 'Average:':
            continue
        (time, usr, nice, sys, idle) = l
        l = [time, 'all', usr, nice, sys, '0', '0', '0', '0', '0', idle, rcEnv.nodename]
        l[0] = '%s %s'%(d, l[0])
        lines.append(l)
    return lines

def stats_mem_u(file, collect_date=None):
    return twodays(stats_mem_u_day)

def stats_mem_u_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    fname = customfile('mem_u', day)
    if fname is None:
        return []
    try:
        f = open(fname, 'r')
        buff = f.read()
        f.close()
    except:
        return []
    lines = []
    for line in buff.split('\n'):
        l = line.split()
        if len(l) != 6:
            continue
        (time, free, inactive, active, speculative, wired) = l
        l = [time, free, active, '0', speculative, inactive, '0', '0', wired, rcEnv.nodename]
        l[0] = '%s %s'%(d, l[0])
        lines.append(l)
    return lines

def stats_proc(file, collect_date=None):
    return twodays(stats_proc_day)

def stats_proc_day(t):
    return []

def stats_swap(file, collect_date=None):
    return twodays(stats_swap_day)

def stats_swap_day(t):
    return []

def stats_block(file, collect_date=None):
    return twodays(stats_block_day)

def stats_block_day(t):
    return []

def stats_blockdev(file, collect_date=None):
    return twodays(stats_blockdev_day)

def stats_blockdev_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-d', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
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
       l += ['0', '0', '0', '0', '0', '0']
       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines

def stats_netdev(file, collect_date=None):
    return twodays(stats_netdev_day)

def stats_netdev_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-n', 'DEV', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 6:
           continue
       if l[1] in ['IFACE', 'lo0'] :
           continue
       if 'dummy' in l[1] or 'vnet' in l[1] or 'veth' in l[1] or \
          'gif'   in l[1] or 'stf'  in l[1]:
           continue
       if l[0] == 'Average:':
           continue
       (time, dev, ipckps, ibps, opckps, obps) = l
       l = [time, dev, ipckps, opckps, ibps, obps, rcEnv.nodename]
       l[0] = '%s %s'%(d, time)
       lines.append(l)
    return lines


def stats_netdev_err(file, collect_date=None):
    return twodays(stats_netdev_err_day)

def stats_netdev_err_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-n', 'EDEV', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 6:
           continue
       if l[1] in ['IFACE', 'lo0'] :
           continue
       if 'dummy' in l[1] or 'vnet' in l[1] or 'veth' in l[1] or \
          'gif'   in l[1] or 'stf'  in l[1]:
           continue
       if l[0] == 'Average:':
           continue
       (time, dev, ierrps, oerrps, collps, dropps) = l
       l = [time, dev, ierrps, oerrps, collps, dropps, '0', rcEnv.nodename]
       l[0] = '%s %s'%(d, time)
       lines.append(l)
    return lines


