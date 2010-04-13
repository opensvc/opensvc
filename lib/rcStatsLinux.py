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

def stats_cpu():
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_cpu_day(t)
    t = datetime.datetime.today()
    lines += stats_cpu_day(t)
    return lines

def stats_cpu_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
        return []
    cmd = ['sar', '-t', '-u', 'ALL', '-P', 'ALL', '-f', f]
    (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
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
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_mem_u_day(t)
    t = datetime.datetime.today()
    lines += stats_mem_u_day(t)
    return lines

def stats_mem_u_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
        return []
    cmd = ['sar', '-t', '-r', '-f', f]
    (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 8:
           continue
       if l[1] == 'kbmemfree':
           continue
       if l[0] == 'Average:':
           continue
       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines

def stats_proc():
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_proc_day(t)
    t = datetime.datetime.today()
    lines += stats_proc_day(t)
    return lines

def stats_proc_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
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
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_swap_day(t)
    t = datetime.datetime.today()
    lines += stats_swap_day(t)
    return lines

def stats_swap_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
        return []
    cmd = ['sar', '-t', '-S', '-f', f]
    (ret, buff) = call(cmd)
    lines = []
    for line in buff.split('\n'):
       l = line.split()
       if len(l) != 6:
           continue
       if l[1] == 'kbswpcad':
           continue
       if l[0] == 'Average:':
           continue
       l.append(rcEnv.nodename)
       l[0] = '%s %s'%(d, l[0])
       lines.append(l)
    return lines

def stats_block():
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_block_day(t)
    t = datetime.datetime.today()
    lines += stats_block_day(t)
    return lines

def stats_block_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
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

def stats_block():
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_block_day(t)
    t = datetime.datetime.today()
    lines += stats_block_day(t)
    return lines

def stats_block_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
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

def stats_block():
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_block_day(t)
    t = datetime.datetime.today()
    lines += stats_block_day(t)
    return lines

def stats_block_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
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

def stats_block():
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_block_day(t)
    t = datetime.datetime.today()
    lines += stats_block_day(t)
    return lines

def stats_block_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
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

def stats_block():
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_block_day(t)
    t = datetime.datetime.today()
    lines += stats_block_day(t)
    return lines

def stats_block_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
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

def stats_block():
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_block_day(t)
    t = datetime.datetime.today()
    lines += stats_block_day(t)
    return lines

def stats_block_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
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
    if which('sar') is None:
        return []
    t = datetime.datetime.today() - datetime.timedelta(days=1)
    lines = stats_blockdev_day(t)
    t = datetime.datetime.today()
    lines += stats_blockdev_day(t)
    return lines

def stats_blockdev_day(t):
    d = t.strftime("%Y-%m-%d")
    day = t.strftime("%d")
    f = os.path.join(os.sep, 'var', 'log', 'sysstat', 'sa'+day)
    if not os.path.exists(f):
        return []
    cmd = ['sar', '-t', '-d', '-p', '-f', f]
    (ret, buff) = call(cmd)
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


