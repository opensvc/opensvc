import provisioning
import os

import rcExceptions as ex
from svc import Svc
from converters import print_size

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        if not self.r.volsvc.exists():
            return False
        if not self.owned():
            return False
        return self.r.volsvc.print_status_data()["provisioned"]

    def claimed(self, volume=None):
        if not volume:
            volume = self.r.volsvc
        if volume.children:
            return True
        return False

    def owned(self, volume=None):
        if not volume:
            volume = self.r.volsvc
        if not self.claimed(volume):
            return False
        if self.r.svc.svcpath not in volume.children:
            return False
        return True

    def owned_exclusive(self, volume=None):
        if not volume:
            volume = self.r.volsvc
        if set(volume.children) != set([self.r.svc.svcpath]):
            return False
        return True

    def claim(self, volume):
        if self.r.shared:
            if self.owned_exclusive(volume):
                self.r.log.info("volume %s is already claimed exclusively by %s",
                                volume.svcpath, self.r.svc.svcpath)
                return
            if self.claimed(volume):
                raise ex.excError("shared volume %s is already claimed by %s" % (volume.svcname, ",".join(volume.children)))
        else:
            if self.owned(volume):
                self.r.log.info("volume %s is already claimed by %s",
                                volume.svcpath, self.r.svc.svcpath)
                return
        self.r.log.info("claim volume %s", volume.svcname)
        volume.set_multi(["DEFAULT.children+=%s" % self.r.svc.svcpath])

    def unclaim(self, volume):
        self.r.log.info("unclaim volume %s", volume.svcname)
        volume.set_multi(["DEFAULT.children-=%s" % self.r.svc.svcpath])

    def unprovisioner(self):
        if not self.r.volsvc.exists():
            return
        self.r.volsvc.set_multi(["DEFAULT.children-=%s" % self.r.svc.svcpath])

    def provisioner(self):
        """
        Create a volume service with resources definitions deduced from the storage
        pool translation rules.
        """
        volume = self.create_volume()
        self.claim(volume)

        # will be rolled back by the volume resource. for now, the remaining
        # resources might need the volume for their provision
        ret = volume.action("provision", options={
            "disable_rollback": True,
            "local": True,
            "leader": self.r.svc.options.leader
        }) 
        if ret != 0:
            raise ex.excError
        volume.freezer.thaw()
        self.r.unset_lazy("mount_point")
        self.r.unset_lazy("volsvc")

    def create_volume(self):
        name = self.r.conf_get("name")
        volume = Svc(svcname=name, namespace=self.r.svc.namespace, node=self.r.svc.node)
        if volume.exists():
            self.r.log.info("volume %s already exists", name)
            data = volume.print_status_data(mon_data=True)
            if not data or "cluster" not in data:
                return volume
            if self.r.svc.options.leader and volume.topology == "failover" and \
               (self.owned() or not self.claimed(volume)) and \
               data["avail"] != "up" and data["cluster"]["avail"] == "up":
                self.r.log.info("volume %s is up on peer, we are leader: take it over", name)
                volume.action("takeover", options={"wait": True, "time": 60})
            return volume
        elif not self.r.svc.options.leader:
            self.r.log.info("volume %s does not exist, we are not leader: wait its propagation", name)
            self.r.wait_for_fn(lambda: volume.exists(), 10, 1,
                               "non leader instance waited for too long for the "
                               "volume to appear")
            return volume
        try:
            pooltype = self.r.conf_get("type")
        except ex.OptNotFound:
            pooltype = None
        self.r.log.info("create new volume %s (pool name: %s, pool type: %s, "
                        "access: %s, size: %s, format: %s)",
                        name, self.r.pool, pooltype, self.r.access,
                        print_size(self.r.size, unit="B", compact=True),
                        self.r.format)
        pool = self.r.svc.node.find_pool(poolname=self.r.pool,
                                         pooltype=pooltype,
                                         access=self.r.access,
                                         size=self.r.size,
                                         fmt=self.r.format)
        if pool is None:
            raise ex.excError("could not find a pool maching criteria")
        try:
            nodes = self.r.svc._get("DEFAULT.nodes")
        except ex.OptNotFound:
            nodes = None
        pool.configure_volume(volume,
                              fmt=self.r.format,
                              size=self.r.size,
                              access=self.r.access,
                              nodes=nodes)
        return volume

