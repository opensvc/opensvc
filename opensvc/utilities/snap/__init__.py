import os

import core.exceptions as ex

from core.resource import Resource


def find_mount(rs, dir):
    """Sort mounts from deepest to shallowest and return the
       first mount whose 'mount_point' is matching 'dir'
    """
    for m in sorted(rs.resources, reverse=True):
        if m.is_disabled():
            continue
        if m.mount_point in dir:
            return m
    return None

def find_mounts(self, mounts_h):
    rs = self.svc.get_resourcesets("fs")[0]
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

class Snap(Resource):
    """Defines a snap object
    """
    def __init__(self, rid, optional=False, disabled=False, tags=None):
        self.snaps = {}
        super(Snap, self).__init__(
            rid,
            "sync.snap",
            optional=optional,
            disabled=disabled,
            tags=tags or set()
        )

    def try_snap(self, rset, action, rid=None):
        if action == "nodes":
            action = "sync_nodes"
        if action == "drpnodes":
            action = "sync_drp"

        mounts_h = {}
        for r in rset.resources:
            """ if rid is set, snap only the specified resource.
                Used by resources tagged 'delay_snap' on sync()

                if rid is not set, don't snap resources tagged 'delay_snap'
                (pre_action() code path)
            """
            if rid is None:
                if "delay_snap" in r.tags:
                    continue
            elif rid != r.rid:
                continue

            if r.is_disabled():
                continue

            if r.snap is not True and r.snap is not False:
                self.log.error("service configuration error: 'snap' must be 'true' or 'false'. default is 'false'")
                raise ex.syncConfigSyntaxError

            if not r.snap:
                continue

            if (action == "sync_nodes" and not 'nodes' in r.target) or \
               (action == "sync_drp" and not 'drpnodes' in r.target):
                self.log.debug("action %s but resource target is %s"%(action, r.target))
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
                self.snap_cleanup(rset)
                raise ex.Error
            except:
                raise

        """Update src dirs of every sync resource to point to an
           existing snap
        """
        for i, r in enumerate(rset.resources):
            r.alt_src = list(r.src)
            for j, src in enumerate(r.alt_src):
                if src not in mounts_h:
                    continue
                mnt = mounts_h[src].mount_point
                if mnt not in self.snaps:
                    continue
                snap_mnt = self.snaps[mnt]['snap_mnt']
                rset.resources[i].alt_src[j] = src.replace(os.path.join(mnt), os.path.join(snap_mnt), 1)

    def snap_cleanup(self, rset=None):
        if not hasattr(self, 'snaps'):
            return
        if len(self.snaps) == 0 :
            return
        for s in list(self.snaps.keys()):
            self.snapdestroykey(s)
        if rset is None:
            return
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

