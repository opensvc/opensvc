import provisioning
import os

import rcExceptions as ex
from rcUtilities import factory
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

    def unclaim(self):
        self.r.log.info("unclaim volume %s", self.r.volsvc.svcname)
        self.r.volsvc.set_multi(["DEFAULT.children-=%s" % self.r.svc.svcpath], validation=False)

    def unprovisioner(self):
        if not self.r.volsvc.exists():
            return
        self.unclaim()

    def provisioner_shared_non_leader(self):
        self.provisioner()

    def provisioner(self):
        """
        Create a volume service with resources definitions deduced from the storage
        pool translation rules.
        """
        volume = self.create_volume()
        self.claim(volume)
        self.r.log.info("provision the %s volume service instance", self.r.volname)

        # will be rolled back by the volume resource. for now, the remaining
        # resources might need the volume for their provision
        ret = volume.action("provision", options={
            "disable_rollback": True,
            "local": True,
            "leader": self.r.svc.options.leader,
            "notify": True,
        }) 
        if ret != 0:
            raise ex.excError("volume provision returned %d" % ret)
        self.r.unset_lazy("device")
        self.r.unset_lazy("mount_point")
        self.r.unset_lazy("volsvc")

    def create_volume(self):
        volume = factory("vol")(svcname=self.r.volname, namespace=self.r.svc.namespace, node=self.r.svc.node)
        if volume.exists():
            self.r.log.info("volume %s already exists", self.r.volname)
            data = volume.print_status_data(mon_data=True)
            if not data or "cluster" not in data:
                return volume
            if not self.r.svc.node.get_pool(volume.pool):
                raise ex.excError("pool %s not found on this node" % volume.pool)
            if self.r.svc.options.leader and volume.topology == "failover" and \
               (self.owned() or not self.claimed(volume)) and \
               data["avail"] != "up" and data["cluster"]["avail"] == "up":
                self.r.log.info("volume %s is up on peer, we are leader: take it over", self.r.volname)
                volume.action("takeover", options={"wait": True, "time": 60})
            return volume
        elif not self.r.svc.options.leader:
            self.r.log.info("volume %s does not exist, we are not leader: wait its propagation", self.r.volname)
            self.r.wait_for_fn(lambda: volume.exists(), 10, 1,
                               "non leader instance waited for too long for the "
                               "volume to appear")
            return volume
        pooltype = self.r.oget("type")
        self.r.log.info("create new volume %s (pool name: %s, pool type: %s, "
                        "access: %s, size: %s, format: %s, shared: %s)",
                        self.r.volname, self.r.pool, pooltype, self.r.access,
                        print_size(self.r.size, unit="B", compact=True),
                        self.r.format, self.r.shared)
        pool = self.r.svc.node.find_pool(poolname=self.r.pool,
                                         pooltype=pooltype,
                                         access=self.r.access,
                                         size=self.r.size,
                                         fmt=self.r.format,
                                         shared=self.r.shared)
        if pool is None:
            raise ex.excError("could not find a pool maching criteria")
        pool.log = self.r.log
        try:
            nodes = self.r.svc._get("DEFAULT.nodes")
        except ex.OptNotFound:
            nodes = None
        env = {}
        for mapping in pool.volume_env:
            try:
                src, dst = mapping.split(":", 1)
            except Exception:
                continue
            args = src.split(".", 1)
            val = self.r.svc.oget(*args)
            if ".." in val:
                raise ex.excError("the '..' substring is forbidden in volume env keys: %s=%s" % (mapping, val))
            env[dst] = val
        pool.configure_volume(volume,
                              fmt=self.r.format,
                              size=self.r.size,
                              access=self.r.access,
                              nodes=nodes,
                              shared=self.r.shared,
                              env=env)
        volume = factory("vol")(svcname=self.r.volname, namespace=self.r.svc.namespace, node=self.r.svc.node)
        return volume

