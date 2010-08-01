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

(ret, out) = call(['dmidecode'])
if ret != 0:
    dmidecode = []
else:
    dmidecode = out.split('\n')

def get_mem_bytes():
    cmd = ['free', '-m']
    (ret, out) = call(cmd)
    if ret != 0:
        return '0'
    lines = out.split('\n')
    if len(lines) < 2:
        return '0'
    line = lines[1].split()
    if len(line) < 2:
        return '0'
    return line[1]

def get_mem_banks():
    banks =  0
    inBlock = False
    for l in dmidecode:
        if not inBlock and l == "Memory Device":
             inBlock = True
        if inBlock and "Size:" in l:
             e = l.split()
             if len(e) == 3:
                 try:
                     size = int(e[1])
                     banks += 1
                 except:
                     pass
    return str(banks)

def get_mem_slots():
    for l in dmidecode:
        if 'Number Of Devices:' in l:
            return l.split()[-1]
    return '0'

def get_os_vendor():
    if os.path.exists('/etc/debian_version'):
        return 'Debian'
    if os.path.exists('/etc/redhat_release'):
        return 'Redhat'
    if os.path.exists('/etc/centos_release'):
        return 'Centos'
    return 'Unknown'

def get_os_release():
    files = ['/etc/debian_version',
             '/etc/redhat_release',
             '/etc/centos_release']
    for f in files:
        if os.path.exists(f):
            (ret, out) = call(['cat', f])
            if ret != 0:
                return 'Unknown'
            return out.split('\n')[0]
    return 'Unknown'

def get_os_kernel():
    (ret, out) = call(['uname', '-r'])
    if ret != 0:
        return 'Unknown'
    return out.split('\n')[0]

def get_os_arch():
    (ret, out) = call(['arch'])
    if ret != 0:
        return 'Unknown'
    return out.split('\n')[0]

def get_cpu_freq():
    for l in dmidecode:
        if 'Max Speed:' in l:
            return ' '.join(l.split()[-2:])
    return 'Unknown'

def get_cpu_cores():
    c = 0
    for l in dmidecode:
        if 'Core Count:' in l:
            c += int(l.split()[-1])
    return str(c)

def get_cpu_dies():
    c = 0
    for l in dmidecode:
        if 'Processor Information' in l:
            c += 1
    return str(c)

def get_cpu_model():
    (ret, out) = call(['grep', 'model name', '/proc/cpuinfo'])
    if ret != 0:
        return 'Unknown'
    lines = out.split('\n')
    l = lines[0].split(':')
    return l[1].strip()

def get_serial():
    for l in dmidecode:
        if 'Serial Number:' in l:
            return l.split(':')[-1].strip()
    return 'Unknown'

def get_model():
    for l in dmidecode:
        if 'Product Name:' in l:
            return l.split(':')[-1].strip()
    return 'Unknown'

def get_environnement():
    f = os.path.join(rcEnv.pathvar, 'host_mode')
    if os.path.exists(f):
        (ret, out) = call(['cat', f])
        if ret != 0:
            return 'Unknown'
    return out.split('\n')[0]

def get_asset_dict():
    d = {}
    d['nodename'] = rcEnv.nodename
    d['os_name'] = rcEnv.sysname
    d['os_vendor'] = get_os_vendor()
    d['os_release'] = get_os_release()
    d['os_kernel'] = get_os_kernel()
    d['os_arch'] = get_os_arch()
    d['mem_bytes'] = get_mem_bytes()
    d['mem_banks'] = get_mem_banks()
    d['mem_slots'] = get_mem_slots()
    d['cpu_freq'] = get_cpu_freq()
    d['cpu_cores'] = get_cpu_cores()
    d['cpu_dies'] = get_cpu_dies()
    d['cpu_model'] = get_cpu_model()
    d['serial'] = get_serial()
    d['model'] = get_model()
    d['environnement'] = get_environnement()
    return d
