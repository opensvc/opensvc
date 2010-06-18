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
from osol_install.distro_const.dc_checkpoint import snapshot_list
from rcUtilities import justcall
"""
"""

def dataset_exists(device, type):
    "return True if dataset exists else return False"
    (out, err, ret) = justcall('zfs get -H -o value type'.split()+[device])
    if ret == 0 and out.split('\n')[0] == type :
        return True
    else:
        return False

def zfs_getprop(dataset='undef_ds', propname='undef_prop'):
    cmd = [ 'zfs', 'get', '-Hp', '-o', 'value', propname, dataset ]
    (stdout, stderr, retcode) = justcall(cmd)
    if retcode == 0 :
        return stdout.split("\n")[0]
    else:
        return ""

def zfs_setprop(dataset='undef_ds', propname='undef_prop', propval='undef_val'):
    if zfs_getprop(dataset, propname) == propval :
        return True
    cmd = [ 'zfs', 'set', propname + '='+ propval, dataset ]
    print ' '.join(cmd)
    (stdout, stderr, retcode) = justcall(cmd)
    if retcode == 0 :
        return True
    else:
        print 'status: ' , retcode
        print 'stdout: ' + stdout
        print 'stderr: ' + stderr
        return False

def zfs_send(dataset='undef_ds',dest_nodes=None,mode='cluster', verbose=False):
            send):
    if mode == 'cluster':
        snapprefix = 'dup2ls'
    else:
        snapprefix = 'dup2ls_backup'

    snapname_base = dataset + '@' + snapprefix
    snap_tosend = snapname_base + '_tosend'
    snap_sent = snapname_base + '_sent'

    if not dataset_exists(snap_tosent, 'snapshot' ) :
        cmd = split('zfs snapshot ' + snap_tosend)
        print ' '
        (stdout, stderr, retcode) = justcall(cmd)
        if retcode == 0 :
            print
