#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
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
    
from rcGlobalEnv import rcEnv
from rcUtilities import which, qcall, protected_mount
import exceptions as ex

def find_mount(rs, dir):
    """Sort mounts from deepest to shallowest and return the
       first mount whose 'mountPoint' is matching 'dir'
    """
    for m in sorted(rs.resources, reverse=True):
        if m.mountPoint in dir:
            return m
    return None

def find_mounts(self, mounts_h):
    rs = self.svc.get_res_sets("mount")[0]
    if rs is None:
        self.log.error("can not find mount resources encapsulating %s to snap"%r.src)
        raise ex.syncNotSnapable
    for src in self.src:
        m = find_mount(rs, src)
        if m is None:
            self.log.error("can not find mount resources encapsulating %s to snap"%self.src)
            raise ex.syncNotSnapable
        mounts_h[src] = m
    return mounts_h

def lv_exists(self, device):
    if qcall(['lvs', device]) == 0:
        return True
    return False

def lv_info(self, device):
    (ret, buff) = self.call(['lvs', '-o', 'vg_name,lv_name,lv_size', '--noheadings', '--units', 'm', device])
    if ret != 0:
        return (None, None, None)
    info = buff.split()
    lv_size = float(info[2].split('M')[0])
    return (info[0], info[1], lv_size)

def snap(self, rset, action):
    mounts_h = {}
    for r in rset.resources:
        if r.snap is not True and r.snap is not False:
            r.log.error("service configuration error: 'snap' must be 'true' or 'false'. default is 'false'")
            raise ex.syncConfigSyntaxError

        if not r.snap:
            continue

        if (action == "syncnodes" and not 'nodes' in r.target) or \
           (action == "syncdrp" and not 'drpnodes' in r.target):
            continue

        mounts_h = find_mounts(r, mounts_h)

    mounts = set(mounts_h.values())
    snaps = {}
    try:
        for m in mounts:
            snap_name = ''
            snap_mnt = ''
            (vg_name, lv_name, lv_size) = lv_info(self, m.device)
            if lv_name is None:
                self.log.error("can not snap %s: not a logical volume"%m.device)
                raise ex.syncNotSnapable
            snap_name = 'osvc_sync_'+lv_name
            if lv_exists(self, os.path.join(os.sep, 'dev', vg_name, snap_name)):
                self.log.error("snap of %s already exists"%(lv_name))
                raise ex.syncSnapExists
            (ret, buff) = self.vcall(['lvcreate', '-s', '-L'+str(lv_size//10)+'M', '-n', snap_name, os.path.join(vg_name, lv_name)])
            if ret != 0:
                raise ex.syncSnapCreateError
            snap_mnt = '/service/tmp/osvc_sync_'+vg_name+'_'+lv_name
            if not os.path.exists(snap_mnt):
                os.makedirs(snap_mnt, 0755)
            snap_dev = os.path.join(os.sep, 'dev', vg_name, snap_name)
            (ret, buff) = self.vcall(['mount', '-o', 'ro', snap_dev, snap_mnt])
            if ret != 0:
                raise ex.syncSnapMountError
            snaps[m.mountPoint] = dict(lv_name=lv_name,
                                       vg_name=vg_name,
                                       snap_name=snap_name,
                                       snap_mnt=snap_mnt,
                                       snap_dev=snap_dev)
    except (ex.syncNotSnapable, ex.syncSnapExists, ex.syncSnapMountError):
        """Clean up the mess
        """
        snap_cleanup(self, snaps)
        raise ex.excError
    except:
        raise

    """Update src dirs of every sync resource to point to an
       existing snap
    """
    for i, r in enumerate(rset.resources):
        r.alt_src = list(r.src)
        for j, src in enumerate(r.alt_src):
            if not mounts_h.has_key(src):
                continue
            mnt = mounts_h[src].mountPoint
            snap_mnt = snaps[mnt]['snap_mnt']
            rset.resources[i].alt_src[j] = src.replace(os.path.join(mnt), os.path.join(snap_mnt), 1)
 
    return snaps

def snap_cleanup(self, rset):
    if not hasattr(rset, 'snaps'):
        return
    snaps = rset.snaps
    for s in snaps.keys():
        if protected_mount(snaps[s]['snap_mnt']):
            self.log.error("the snapshot is no longer mounted in %s. panic."%snaps[s]['snap_mnt'])
            raise ex.excError
        cmd = ['fuser', '-kmv', snaps[s]['snap_mnt']]
        (ret, out) = self.vcall(cmd)
        cmd = ['umount', snaps[s]['snap_mnt']]
        (ret, out) = self.vcall(cmd)
        cmd = ['lvremove', '-f', snaps[s]['snap_dev']]
        (ret, buff) = self.vcall(cmd)

    for i, r in enumerate(rset.resources):
        if hasattr(rset.resources[i], 'alt_src'):
            delattr(rset.resources[i], 'alt_src')

