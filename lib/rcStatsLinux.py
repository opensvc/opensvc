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

def sarfile(t):
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if os.path.exists(f):
        return f
    f = os.path.join(os.sep, 'var', 'log', 'sa', 'sa'+day)
    if os.path.exists(f):
        return f
    return None

def dofile(fn, file, collect_date=None):
    if which('sar') is None:
        return []
    if collect_date is None:
       collect_date = today
    else:
       try:
           collect_date = datetime.datetime.strptime(collect_date, "%Y-%m-%d")
       except:
           print "collect date format is %Y-%m-%d"
           raise
    i = len(file)-1
    while i>0:
        try:
            j = int(file[i])
        except:
            break
        i -= 1
    if i == 0 or i == len(file)-1:
        return []
    file_day = int(file[i+1:])
    file_month = collect_date.month
    file_year = collect_date.year
    if file_day > collect_date.day:
        file_month -= 1
        if file_month == 0:
            file_month = 12
            file_year -= 1
    day = datetime.date(file_year, file_month, file_day)
    print file, day
    lines = fn(day, file)
    return lines

def twodays(fn):
    if which('sar') is None:
        return []
    file = sarfile(yesterday)
    lines = []
    if file is not None:
        lines += fn(yesterday, file)
    file = sarfile(today)
    if file is not None:
        lines += fn(today, file)
    return lines

def stats_cpu(file, collect_date=None):
    if file is not None and os.path.exists(file):
        return dofile(stats_cpu_day, file, collect_date)
    return twodays(stats_cpu_day)

def stats_cpu_day(t, f):
    d = t.strftime("%Y-%m-%d")
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
        if len(l) == 7:
            """ redhat 4
            """
            l = l[0:6] + ['0', '0', '0', '0', l[6]]
        elif len(l) != 11:
            continue
        if l[1] == 'CPU':
            continue
        if l[0] == 'Average:':
            continue
        l.append(rcEnv.nodename)
        l[0] = '%s %s'%(d, l[0])
        lines.append(l)
    return lines

def stats_mem_u(file, collect_date=None):
    if file is not None and os.path.exists(file):
        return dofile(stats_mem_u_day, file, collect_date)
    return twodays(stats_mem_u_day)

def stats_mem_u_day(t, f):
    d = t.strftime("%Y-%m-%d")
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

def stats_proc(file, collect_date=None):
    if file is not None and os.path.exists(file):
        return dofile(stats_proc_day, file, collect_date)
    return twodays(stats_proc_day)

def stats_proc_day(t, f):
    d = t.strftime("%Y-%m-%d")
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

def stats_swap(file, collect_date=None):
    if file is not None and os.path.exists(file):
        return dofile(stats_swap_day, file, collect_date)
    return twodays(stats_swap_day)

def stats_swap_day(t, f):
    d = t.strftime("%Y-%m-%d")
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

def stats_block(file, collect_date=None):
    if file is not None and os.path.exists(file):
        return dofile(stats_block_day, file, collect_date)
    return twodays(stats_block_day)

def stats_block_day(t, f):
    d = t.strftime("%Y-%m-%d")
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

def stats_blockdev(file, collect_date=None):
    if file is not None and os.path.exists(file):
        return dofile(stats_blockdev_day, file, collect_date)
    return twodays(stats_blockdev_day)

def stats_blockdev_day(t, f):
    d = t.strftime("%Y-%m-%d")
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

def stats_netdev(file, collect_date=None):
    if file is not None and os.path.exists(file):
        return dofile(stats_netdev_day, file, collect_date)
    return twodays(stats_netdev_day)

def stats_netdev_day(t, f):
    d = t.strftime("%Y-%m-%d")
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


def stats_netdev_err(file, collect_date=None):
    if file is not None and os.path.exists(file):
        return dofile(stats_netdev_err_day, file, collect_date)
    return twodays(stats_netdev_err_day)

def stats_netdev_err_day(t, f):
    d = t.strftime("%Y-%m-%d")
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


