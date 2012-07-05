#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import sys
import glob

def print_diskinfo_header():
    print "%-12s %-8s %12s MB %-8s %-8s %-16s"%(
      "hbtl",
      "devname",
      "size",
      "dev",
      "vendor",
      "model",
    )

def print_diskinfo(disk):
    name = os.path.basename(disk)
    info = {
      'dev': '',
      'size': 0,
      'device/vendor': '',
      'device/model': '',
    }
    for i in info:
        i_f = os.path.join(disk, i)
        if not os.path.exists(i_f):
            continue
        with open(i_f, 'r') as f:
            info[i] = f.read().strip()
    info['hbtl'] = os.path.basename(os.path.realpath(os.path.join(disk, "device")))
    print "%-12s %-8s %12s MB %-8s %-8s %-16s"%(
      info['hbtl'],
      name,
      int(float(info['size'])/2//1024),
      info['dev'],
      info['device/vendor'],
      info['device/model'],
    )

def scanscsi():
    if not os.path.exists('/sys') or not os.path.ismount('/sys'):
        print >>sys.stderr, "scanscsi is not supported without /sys mounted"
        return 1

    disks_before = glob.glob('/sys/block/sd*')
    hosts = glob.glob('/sys/class/scsi_hosts/host*')

    for host in hosts:
        scan_f = host+'/scan'
        if not os.path.exists(scan_f):
            continue
        print "scan", os.path.basename(host)
        os.command('echo - - - >'+scan_f)
    
    disks_after = glob.glob('/sys/block/sd*')
    new_disks = set(disks_after) - set(disks_before)

    print_diskinfo_header()
    #for disk in disks_before:
    for disk in new_disks:
        print_diskinfo(disk)

