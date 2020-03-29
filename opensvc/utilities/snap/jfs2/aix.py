import os

import core.exceptions as ex
import utilities.snap
from utilities.files import protected_mount
from utilities.proc import qcall

class Snap(utilities.snap.Snap):
    def lv_exists(self, device):
        device = device.split("/")[-1]
        ret = qcall(['lslv', device])
        if ret == 0:
            return True
        return False

    def lv_info(self, device):
        device = device.split("/")[-1]
        (ret, buff, err) = self.call(['lslv', device], cache=True)
        if ret != 0:
            return (None, None, None)
        vg_name = None
        lv_name = None
        lv_size = 0
        prev = ''
        prevprev = ''
        pp_unit = ''
        pps = 0
        pp_size = 0
        for word in buff.split():
            if prev == "GROUP:":
                vg_name = word
            if prev == "VOLUME:":
                lv_name = word
            if prev == "SIZE:":
                pp_size = int(word)
            if prevprev == "SIZE:":
                pp_unit = word
            if prev == "PPs:" and prevprev != "STALE":
                pps = int(word)
            prevprev = prev
            prev = word

        if pps == 0 or pp_size == 0 or pp_unit == '' or vg_name is None:
            self.log.error("logical volume %s information fetching error"%device)
            print("pps = ", pps)
            print("pp_size = ", pp_size)
            print("pp_unit = ", pp_unit)
            print("vg_name = ", vg_name)
            raise ex.Error

        if pp_unit == 'megabyte(s)':
            mult = 1
        elif pp_unit == 'gigabyte(s)':
            mult = 1024
        elif pp_unit == 'terabyte(s)':
            mult = 1024*1024
        else:
            self.log.error("unexpected logical volume PP size unit: %s"%pp_unit)
            raise ex.Error

        return (vg_name, lv_name, pps*pp_size*mult)

    def snapcreate(self, m):
        snap_name = ''
        snap_mnt = ''
        (vg_name, lv_name, lv_size) = self.lv_info(m.device)
        if lv_name is None:
            self.log.error("can not snap %s: not a logical volume"%m.device)
            raise ex.syncNotSnapable
        if len(lv_name) > 12:
            self.log.error("can not snap lv with name >12 chars")
            raise ex.Error
        snap_name = 'sy_'+os.path.basename(lv_name)
        if self.lv_exists(os.path.join(vg_name, snap_name)):
            self.log.error("snap of %s already exists"%(lv_name))
            raise ex.syncSnapExists
        print(lv_size)
        print(lv_size//10)
        (ret, buff, err) = self.vcall(['mklv', '-t', 'jfs2', '-y', snap_name, vg_name, str(lv_size//10)+'M'])
        if ret != 0:
            raise ex.syncSnapCreateError
        snap_mnt = '/service/tmp/osvc_sync_'+os.path.basename(vg_name)+'_'+os.path.basename(lv_name)
        if not os.path.exists(snap_mnt):
            os.makedirs(snap_mnt, 0o755)
        snap_dev = os.path.join(os.sep, 'dev', snap_name)
        (ret, buff, err) = self.vcall(['snapshot', '-o', 'snapfrom='+m.mount_point, snap_dev])
        if ret != 0:
            raise ex.syncSnapMountError
        (ret, buff, err) = self.vcall(['mount', '-o', 'snapshot', snap_dev, snap_mnt])
        if ret != 0:
            raise ex.syncSnapMountError
        self.snaps[m.mount_point] = dict(lv_name=lv_name,
                                        vg_name=vg_name,
                                        snap_name=snap_name,
                                        snap_mnt=snap_mnt,
                                        snap_dev=snap_dev)

    def snapdestroykey(self, s):
        if protected_mount(self.snaps[s]['snap_mnt']):
            self.log.error("the snapshot is no longer mounted in %s. panic."%self.snaps[s]['snap_mnt'])
            raise ex.Error

        """ fuser on HP-UX outs to stderr ...
        """
        cmd = ['fuser', '-c', '-x', '-k', self.snaps[s]['snap_mnt']]
        ret = qcall(cmd)

        cmd = ['umount', self.snaps[s]['snap_mnt']]
        (ret, out, err) = self.vcall(cmd)
        cmd = ['snapshot', '-d', self.snaps[s]['snap_dev']]
        (ret, buff, err) = self.vcall(cmd)

