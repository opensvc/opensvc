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
from rcUtilities import protected_mount, justcall
from rcUtilitiesLinux import lv_info, lv_exists
import rcExceptions as ex
import snap

class Snap(snap.Snap):
    def mntopt_and_ro(self, m):
        if m.mntOpt is None:
            return 'ro'
        opt_set = set(m.mntOpt.split(','))
        opt_set -= set(['rw', 'ro'])
        opt_set |= set(['ro'])
        return ','.join(opt_set)

    def snapcreate(self, m):
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

        if m.snap_size is not None:
            snap_size = m.snap_size
        else:
            snap_size = int(lv_size//10)

        out, err, ret = justcall(['lvcreate', '-A', 'n', '-s', '-L'+str(snap_size)+'M', '-n', snap_name, os.path.join(vg_name, lv_name)])
        err_l1 = err.split('\n')
        err_l2 = []
        out_l = out.split('\n')
        for e in err_l1:
            if 'This metadata update is NOT backed up' in e:
                pass
            else:
                err_l2.append(e)
        err = '\n'.join(err_l2)
        out = '\n'.join(out_l)
        if len(out) > 0:
            self.log.info(out)
        if len(err) > 0:
            self.log.error(err)
        if ret != 0:
            raise ex.syncSnapCreateError
        snap_mnt = os.path.join(rcEnv.pathtmp,
                                'osvc_sync_'+vg_name+'_'+lv_name)
        if not os.path.exists(snap_mnt):
            os.makedirs(snap_mnt, 0755)
        snap_dev = os.path.join(os.sep, 'dev', vg_name, snap_name)
        self.vcall(['fsck', '-a', snap_dev], err_to_warn=True)
        (ret, buff, err) = self.vcall(['mount', '-o', self.mntopt_and_ro(m), snap_dev, snap_mnt])
        if ret != 0:
            raise ex.syncSnapMountError
        self.snaps[m.mountPoint] = dict(lv_name=lv_name,
                                        vg_name=vg_name,
                                        snap_name=snap_name,
                                        snap_mnt=snap_mnt,
                                        snap_dev=snap_dev)

    def snapdestroykey(self, s):
        if protected_mount(self.snaps[s]['snap_mnt']):
            self.log.error("the snapshot is no longer mounted in %s. panic."%self.snaps[s]['snap_mnt'])
            raise ex.excError
        cmd = ['fuser', '-kmv', self.snaps[s]['snap_mnt']]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)
        cmd = ['umount', self.snaps[s]['snap_mnt']]
        (ret, out, err) = self.vcall(cmd)

        cmd = ['lvremove', '-A', 'n', '-f', self.snaps[s]['snap_dev']]
        self.log.info(' '.join(cmd))
        for i in range(1, 30):
            out, err, ret = justcall(cmd)
            if ret == 0:
                break
        err_l1 = err.split('\n')
        err_l2 = []
        out_l = out.split('\n')
        for e in err_l1:
            if 'This metadata update is NOT backed up' in e:
                pass
            elif 'Falling back to direct link removal.' in e:
                out_l.append(e)
            elif 'Falling back to direct node removal.' in e:
                out_l.append(e)
            else:
                err_l2.append(e)
        err = '\n'.join(err_l2)
        out = '\n'.join(out_l)
        if len(out) > 0:
            self.log.info(out)
        if len(err) > 0:
            self.log.error(err)
        if ret != 0:
            self.log.error("failed to remove snapshot %s (attempts: %d)"%(self.snaps[s]['snap_dev'], i))
        elif i > 1:
            self.log.info("successfully removed snapshot %s (attempts: %d)"%(self.snaps[s]['snap_dev'], i))
        del(self.snaps[s])

