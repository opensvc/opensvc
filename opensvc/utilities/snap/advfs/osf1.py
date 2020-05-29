import os

import core.exceptions as ex
import utilities.snap
import utilities.subsystems.advfs
from env import Env
from utilities.files import protected_mount
from utilities.mounts.osf1 import Mounts


class Snap(utilities.snap.Snap):
    """Defines a snap object with ZFS
    """

    def snapcreate(self, m):
        """ create a snapshot for m
        add self.snaps[m] with
            dict(snapinfo key val)
        """
        dom, fset = m.device.split('#')
        o = utilities.subsystems.advfs.Fdmns()
        try:
            d = o.get_fdmn(dom)
        except utilities.subsystems.advfs.ExInit:
            raise ex.syncNotSnapable
        if fset not in d.fsets:
            raise ex.syncNotSnapable
        clonefset = fset +'@osvc_sync'
        mount_point = m.mount_point
        snap_mount_point = os.path.join(Env.paths.pathtmp, 'clonefset/%s/%s/osvc_sync'%(m.svc.fullname, mount_point))
        snap_mount_point = os.path.normpath(snap_mount_point)
        if not os.path.exists(snap_mount_point):
            try:
                os.makedirs(snap_mount_point)
                self.log.info('create directory %s'%snap_mount_point)
            except:
                self.log.error('failed to create directory %s'%snap_mount_point)
                raise ex.syncSnapCreateError
        clonedev = '#'.join((dom, clonefset))
        if Mounts().has_mount(clonedev, snap_mount_point):
            cmd = ['fuser', '-kcv', snap_mount_point]
            (ret, out, err) = self.vcall(cmd, err_to_info=True)
            cmd = ['umount', snap_mount_point]
            (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                raise ex.Error
        if clonefset in d.fsets:
            (ret, buff, err) = self.vcall(['rmfset', '-f', dom, clonefset])
            if ret != 0:
                raise ex.syncSnapDestroyError
        (ret, buff, err) = self.vcall(['clonefset', dom, fset, clonefset])
        if ret != 0:
            raise ex.syncSnapCreateError
        (ret, buff, err) = self.vcall(['mount', '-t', 'advfs', clonedev, snap_mount_point])
        if ret != 0:
            raise ex.syncSnapCreateError
        self.snaps[mount_point]={'snap_mnt' : snap_mount_point, \
                                'snapdev' : clonedev }

    def snapdestroykey(self, snap_key):
        """ destroy a snapshot for a mount_point
        """
        clonedev = self.snaps[snap_key]['snapdev']
        dom, clonefset = clonedev.split('#')
        o = utilities.subsystems.advfs.Fdmns()
        try:
            d = o.get_fdmn(dom)
        except utilities.subsystems.advfs.ExInit:
            raise ex.syncSnapDestroyError
        if clonefset not in d.fsets:
            return

        if protected_mount(self.snaps[snap_key]['snap_mnt']):
            self.log.error("the clone fset is no longer mounted in %s. panic."%self.snaps[snap_key]['snap_mnt'])
            raise ex.Error
        cmd = ['fuser', '-kcv', self.snaps[snap_key]['snap_mnt']]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)
        cmd = ['umount', self.snaps[snap_key]['snap_mnt']]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

        (ret, buff, err) = self.vcall(['rmfset', '-f', dom, clonefset])
        if ret != 0:
            raise ex.syncSnapDestroyError
