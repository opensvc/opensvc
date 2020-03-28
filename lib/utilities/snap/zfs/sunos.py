import core.exceptions as ex
import utilities.snap
from env import Env
from utilities.subsystems.zfs import dataset_exists

class Snap(utilities.snap.Snap):
    """Defines a snap object with ZFS
    """

    def snapcreate(self, m):
        """ create a snapshot for m
        add self.snaps[m] with
            dict(snapinfo key val)
        """
        dataset = m.device
        if not dataset_exists(dataset, 'filesystem'):
            raise ex.syncNotSnapable
        snapdev = dataset +'@osvc_sync'
        mount_point = m.mount_point
        snap_mount_point= mount_point + '/.zfs/snapshot/osvc_sync/'
        if dataset_exists(snapdev, 'snapshot'):
            (ret, buff, err) = self.vcall([Env.syspaths.zfs, 'destroy', snapdev ])
            if ret != 0:
                raise ex.syncSnapDestroyError
        (ret, buff, err) = self.vcall([Env.syspaths.zfs, 'snapshot', snapdev ])
        if ret != 0:
            raise ex.syncSnapCreateError
        self.snaps[mount_point]={'snap_mnt' : snap_mount_point, \
                                'snapdev' : snapdev }

    def snapdestroykey(self, snap_key):
        """ destroy a snapshot for a mount_point
        """
        snapdev = self.snaps[snap_key]['snapdev']
        if not dataset_exists(snapdev, 'snapshot'):
            return
        (ret, buff, err) = self.vcall([Env.syspaths.zfs, 'destroy', snapdev ])
        if ret != 0:
            raise ex.syncSnapDestroyError
