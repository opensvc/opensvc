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
from rcUtilities import justcall
import rcExceptions as ex
import snap
from rcVmZfs import dataset_exists

class Snap(snap.Snap):
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
        mountPoint = m.mountPoint
        snapMountPoint= mountPoint + '/.zfs/snapshot/osvc_sync/'
        if dataset_exists(snapdev, 'snapshot'):
            (ret, buff) = self.vcall(['zfs', 'destroy', snapdev ])
            if ret != 0:
                raise ex.syncSnapDestroyError
        (ret, buff) = self.vcall(['zfs', 'snapshot', snapdev ])
        if ret != 0:
            raise ex.syncSnapCreateError
        self.snaps[mountPoint]={'snap_mnt' : snapMountPoint, \
                                'snapdev' : snapdev }

    def snapdestroykey(self, snap_key):
        """ destroy a snapshot for a mountPoint
        """
        snapdev = self.snaps[snap_key]['snapdev']
        if not dataset_exists(snapdev, 'snapshot'):
            return
        (ret, buff) = self.vcall(['zfs', 'destroy', snapdev ])
        if ret != 0:
            raise ex.syncSnapDestroyError