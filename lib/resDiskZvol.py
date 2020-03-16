import os
import rcExceptions as ex
import resDisk
from rcUtilities import lazy
from rcZfs import dataset_exists, zpool_devs
from svcBuilder import init_kwargs


def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    r = Disk(**kwargs)
    svc += r


class Disk(resDisk.Disk):
    def __init__(self,
                 rid=None,
                 name=None,
                 **kwargs):
        resDisk.Disk.__init__(self,
                              rid=rid,
                              name=name,
                              type='disk.zvol',
                              **kwargs)
        self.name = name
        self.label = "zvol %s" % self.name
        if name:
            self.pool = name.split("/", 1)[0]
        else:
            self.pool = None

    def _info(self):
        data = [
          ["name", self.name],
          ["pool", self.pool],
          ["device", self.device],
        ]
        return data

    def has_it(self):
        return dataset_exists(self.name, "volume")

    def is_up(self):
        """
        Returns True if the zvol exists and the pool imported
        """
        return self.has_it()

    def do_start(self):
        pass

    def remove_dev_holders(self, devpath, tree):
        dev = tree.get_dev_by_devpath(devpath)
        if dev is None:
            return
        holders_devpaths = set()
        holder_devs = dev.get_children_bottom_up()
        for holder_dev in holder_devs:
            holders_devpaths |= set(holder_dev.devpath)
        holders_devpaths -= set(dev.devpath)
        holders_handled_by_resources = self.svc.sub_devs() & holders_devpaths
        if len(holders_handled_by_resources) > 0:
            raise ex.excError("resource %s has holders handled by other resources: %s" % (self.rid, ", ".join(holders_handled_by_resources)))
        for holder_dev in holder_devs:
            holder_dev.remove(self)

    def remove_holders(self):
        tree = self.svc.node.devtree
        self.remove_dev_holders(self.device, tree)

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.label)
            return
        self.remove_holders()

    @lazy
    def device(self):
        return "/dev/%s/%s" % (self.pool, self.name.split("/", 1)[-1])

    def sub_devs(self):
        resources = [res for res in self.svc.get_resources("disk.zpool") \
                     if res.name == self.pool]
        if resources:
            return resources[0].sub_devs()
        return set(zpool_devs(self.pool, self.svc.node))

    def exposed_devs(self):
        print(self.device)
        if os.path.exists(self.device):
            return set([self.device])
        return set()

