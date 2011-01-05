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

def stats_cpu():
    return twodays(stats_cpu_day)

def stats_cpu_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-t', '-u', 'ALL', '-P', 'ALL', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    if ret != 0:
        cmd = ['sar', '-t', '-u', '-P', 'ALL', '-f', f]
        (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
        l = line.split()
        if len(l) == 8:
            """ redhat 5
            """
            l = l[0:7] + ['0', '0', '0', l[7]]
        if len(l) != 11:
            continue
        if l[1] == 'CPU':
            continue
        if l[0] == 'Average:':
            continue
        l.append(rcEnv.nodename)
        l[0] = '%s %s'%(d, l[0])
        lines.append(l)
    return lines

def stats_mem_u():
    return twodays(stats_mem_u_day)

def stats_mem_u_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-t', '-r', '-f', f]
    (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) == 10:
           """ redhat 5
           """
           l = l[0:6] + ['0', '0']
       if len(l) != 8:
           continue
       if l[1] == 'kbmemfree':
           continue
       if l[0] == 'Average:':
           continue

       """ Linux has no kbmemsys
       """
       l.append('0')

       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines

def stats_proc():
    return twodays(stats_proc_day)

def stats_proc_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-t', '-q', '-f', f]
    (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 6:
           continue
       if l[1] == 'runq-sz':
           continue
       if l[0] == 'Average:':
           continue
       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines

def stats_swap():
    return twodays(stats_swap_day)

def stats_swap_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-t', '-S', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    if ret != 0:
        """ redhat 5
        """
        cmd = ['sar', '-t', '-r', '-f', f]
        (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) == 10:
           """ redhat 5
           """
           l = [l[0]] + l[6:] + ['0']
       if len(l) != 6:
           continue
       if 'kbswpfree'in l:
           continue
       if l[0] == 'Average:':
           continue
       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines

def stats_block():
    return twodays(stats_block_day)

def stats_block_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-t', '-b', '-f', f]
    (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 6:
           continue
       if l[1] == 'tps':
           continue
       if l[0] == 'Average:':
           continue
       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines

def stats_blockdev():
    return twodays(stats_blockdev_day)

def stats_blockdev_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-t', '-d', '-p', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 10:
           continue
       if l[1] == 'DEV':
           continue
       if l[0] == 'Average:':
           continue
       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines

def stats_netdev():
    return twodays(stats_netdev_day)

def stats_netdev_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-t', '-n', 'DEV', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    div = 1
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 9:
           continue
       if l[1] in ['IFACE', 'lo'] :
           if 'rxbyt/s' in l:
               div = 1024
           continue
       if 'dummy' in l[1] or 'vnet' in l[1] or 'veth' in l[1] or \
          'pan'   in l[1] or 'sit'  in l[1]:
           continue
       if l[0] == 'Average:':
           continue
       m = l[0:3]
       m.append(str(int(float(l[4])/div)))
       m.append(str(int(float(l[5])/div)))
       m.append(l[6])
       m.append(rcEnv.nodename)
       m[0] = '%s %s'%(d, l[0])
       lines.append(m)
    return lines


def stats_netdev_err():
    return twodays(stats_netdev_err_day)

def stats_netdev_err_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = sarfile(day)
    if f is None:
        return []
    cmd = ['sar', '-t', '-n', 'EDEV', '-f', f]
    (ret, buff) = call(cmd, errlog=False)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 11:
           continue
       if l[1] in ['IFACE', 'lo'] :
           continue
       if 'dummy' in l[1] or 'vnet' in l[1] or 'veth' in l[1] or \
          'pan'   in l[1] or 'sit'  in l[1]:
           continue
       if l[0] == 'Average:':
           continue
       l = l[0:7]
       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines


