""" Module providing device path remapping for libvirt VMs
"""
import core.status
import core.exceptions as ex

from .. import BASE_KEYWORDS
from env import Env
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.lazy import lazy

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "vdisk"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "path",
        "required": True,
        "at": True,
        "text": "Path of the device or file used as a virtual machine disk. The path@nodename can be used to to set up different path on each node."
    },
]
DEPRECATED_SECTIONS = {
    "vdisk": ["disk", "vdisk"],
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
)


class DiskVdisk(Resource):
    def __init__(self, name=None, path=None, **kwargs):
        super(DiskVdisk, self).__init__(type="disk.vdisk", **kwargs)
        self.name = name
        self.label = "vdisk %s" % self.name
        self.devpath = path

    def __str__(self):
        return "%s name=%s" % (
            super(DiskVdisk, self).__str__(),
            self.name
        )

    @lazy
    def devpaths(self):
        data = {}
        for n in self.svc.nodes | self.svc.drpnodes:
            data[n] = self.oget("path", impersonate=n)
        return data

    def sub_devs(self):
        return self.devpaths.keys()

    def remap(self):
        path = self.devpaths[Env.nodename]
        paths = set(self.devpaths.values()) - set(self.devpaths[Env.nodename])
        from xml.etree.ElementTree import ElementTree
        tree = ElementTree()
        try:
            tree.parse(self.svc.resources_by_id['container'].cf)
        except:
            self.log.error("failed to parse %s"%self.svc.resources_by_id['container'].cf)
            raise ex.Error
        for dev in tree.getiterator('disk'):
            s = dev.find('source')
            if s is None:
                 continue
            il = s.items()
            if len(il) != 1:
                 continue
            attr, devp = il[0]
            if devp in paths:
                self.log.info("remapping device path: %s -> %s"%(devp,path))
                s.set('dev', path)
                #SubElement(dev, "source", {'dev': path})
                tree.write(self.svc.resources_by_id['container'].cf)

    def stop(self):
        pass

    def start(self):
        self.remap()

    def _status(self, verbose=False):
        return core.status.NA

