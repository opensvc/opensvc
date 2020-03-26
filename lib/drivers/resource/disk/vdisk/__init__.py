""" Module providing device path remapping for libvirt VMs
"""
import core.status
import core.exceptions as ex

from .. import BASE_KEYWORDS
from rcGlobalEnv import rcEnv
from core.resource import Resource
from core.objects.builder import init_kwargs
from core.objects.svcdict import KEYS

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

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    devpath = {}

    for attr, val in svc.cd[s].items():
        if "path@" in attr:
            devpath[attr.replace("path@", "")] = val

    if len(devpath) == 0:
        svc.log.error("path@node must be set in section %s"%s)
        return

    kwargs["devpath"] = devpath
    r = DiskVdisk(**kwargs)
    svc += r


class DiskVdisk(Resource):
    def __init__(self, name=None, devpath={}, **kwargs):
        super(DiskVdisk, self).__init__(type="disk.vdisk", **kwargs)
        self.name = name
        self.label = "vdisk %s" % self.name
        self.devpath = devpath

    def __str__(self):
        return "%s name=%s" % (
            super(DiskVdisk, self).__str__(),
            self.name
        )

    def sub_devs(self):
        return self.devpath.keys()

    def remap(self):
        path = self.devpath[rcEnv.nodename]
        paths = set(self.devpath.values()) - set(self.devpath[rcEnv.nodename])
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

