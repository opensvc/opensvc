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
import resources as Res
import rcExceptions as ex


def find_mount(rs, dir):
    """Sort mounts from deepest to shallowest and return the
       first mount whose 'mountPoint' is matching 'dir'
    """
    for m in sorted(rs.resources, reverse=True):
        if m.is_disabled():
            continue
        if m.mountPoint in dir:
            return m
    return None

def find_mounts(self, mounts_h):
    rs = self.svc.get_res_sets("fs")[0]
    if rs is None:
        self.log.error("can not find fs resources encapsulating %s to snap (no fs resources)"%self.src)
        raise ex.syncNotSnapable
    for src in self.src:
        m = find_mount(rs, src)
        if m is None:
            self.log.error("can not find fs resources encapsulating %s to snap"%src)
            raise ex.syncNotSnapable
        mounts_h[src] = m
    return mounts_h

class Snap(Res.Resource):
    """Defines a snap object
    """
    def __init__(self, rid, optional=False, disabled=False, tags=set([])):
        self.snaps = {}
        Res.Resource.__init__(self, rid, "sync.snap", optional=optional,\
                            disabled=disabled, tags=tags)

    def try_snap(self, rset, action):
        mounts_h = {}
        for r in rset.resources:
            if r.is_disabled():
                continue
            if r.snap is not True and r.snap is not False:
                self.log.error("service configuration error: 'snap' must be 'true' or 'false'. default is 'false'")
                raise ex.syncConfigSyntaxError

            if not r.snap:
                continue

            if (action == "syncnodes" and not 'nodes' in r.target) or \
               (action == "syncdrp" and not 'drpnodes' in r.target):
                continue

            mounts_h = find_mounts(r, mounts_h)

        mounts = set(mounts_h.values())
        for m in mounts:
            try:
                self.snapcreate(m)
            except ex.syncNotSnapable:
                self.log.error("Resource not snapable: "+m.__str__())
                continue
            except (ex.syncNotSnapable, ex.syncSnapExists, ex.syncSnapMountError,
                ex.syncSnapCreateError, ex.syncSnapDestroyError):
                """Clean up the mess
                """
                self.snap_cleanup(snaps)
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
                if not self.snaps.has_key(mnt):
                    continue
                snap_mnt = self.snaps[mnt]['snap_mnt']
                rset.resources[i].alt_src[j] = src.replace(os.path.join(mnt), os.path.join(snap_mnt), 1)

    def snap_cleanup(self, rset):
        if not hasattr(self, 'snaps'):
            return
        snaps = self.snaps
        if len(snaps) == 0 :
            return
        for s in snaps.keys():
            self.snapdestroykey(s)
        for i, r in enumerate(rset.resources):
            if hasattr(rset.resources[i], 'alt_src'):
                delattr(rset.resources[i], 'alt_src')

    def snapcreate(self, m):
        """ create a snapshot for m
        add self.snaps[m] with
            dict(snapinfo key val)
        """
        raise ex.MissImpl

    def snapdestroykey(self, snaps_key):
        """ destroy a snapshot for a snap key
        """
        raise ex.MissImpl

