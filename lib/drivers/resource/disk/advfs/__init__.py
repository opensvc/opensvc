import json
import os
import re

import exceptions as ex

from .. import BaseDisk, BASE_KEYWORDS
from rcGlobalEnv import rcEnv
from svcBuilder import init_kwargs
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "vg"
DRIVER_BASENAME_ALIASES = ["lvm"]
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "name",
        "at": True,
        "required": True,
        "text": "The name of the volume group"
    },
    {
        "keyword": "options",
        "default": "",
        "at": True,
        "provisioning": True,
        "text": "The vgcreate options to use upon vg provisioning."
    },
    {
        "keyword": "pvs",
        "required": True,
        "text": "The list of paths to the physical volumes of the volume group.",
        "provisioning": True
    },
]
DEPRECATED_KEYWORDS = {
    "disk.lvm.vgname": "name",
    "disk.vg.vgname": "name",
}
REVERSE_DEPRECATED_KEYWORDS = {
    "disk.lvm.name": "vgname",
    "disk.vg.name": "vgname",
}
DEPRECATED_SECTIONS = {
    "vg": ["disk", "vg"],
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
    deprecated_keywords=DEPRECATED_KEYWORDS,
    reverse_deprecated_keywords=REVERSE_DEPRECATED_KEYWORDS,
    driver_basename_aliases=DRIVER_BASENAME_ALIASES,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    r = DiskAdvfs(**kwargs)
    svc += r

class DiskAdvfs(BaseDisk):
    def __init__(self, **kwargs):
        super(DiskAdvfs, self).__init__(type='disk.advfs', **kwargs)
        self.label = "fdmn %s" % self.name
        self.sub_devs_cache = set()

    def sub_devs_name(self):
        return os.path.join(self.var_d, 'sub_devs')

    def files_to_sync(self):
        return [self.sub_devs_name()]

    def presync(self):
        """ this one is exported as a service command line arg
        """
        dl = self._sub_devs()
        with open(self.sub_devs_name(), 'w') as f:
            json.dump(list(dl), f)

    def has_it(self):
        """Returns True if the pool is present
        """
        if os.path.exists("/etc/fdmns/"+self.name):
            return True
        return False

    def is_up(self):
        """Returns True if the fdmn is present and activated
        """
        if not self.has_it():
            return False
        cmd = [ 'showfdmn', self.name ]
        out, err, ret = justcall(cmd)
        if ret != 0:
            if len(err) > 0:
                self.status_log(err)
            return False
        if 'not active' in out:
            return False
        return True

    def do_start(self):
        pass

    def do_stop(self):
        pass

    def sub_devs(self):
        if not os.path.exists(self.sub_devs_name()):
            s = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))
            import rcStatus
            if s['overall'].status == rcStatus.UP:
                self.log.debug("no sub_devs cache file and service up ... refresh sub_devs cache")
                self.presync()
            else:
                self.log.debug("no sub_devs cache file and service not up ... unable to evaluate sub_devs")
                return set()
        try:
            with open(self.sub_devs_name(), 'r') as f:
                return set(json.load(f))
        except:
            self.log.error("corrupted sub_devs cache file %s"%self.sub_devs_name())
            raise ex.Error

    def _sub_devs(self):
        # return cache if initialized
        if len(self.sub_devs_cache) > 0:
            return self.sub_devs_cache

        if not os.path.exists("/etc/fdmns/"+self.name):
            return set()

        import glob
        dl = glob.glob("/etc/fdmns/"+self.name+"/*")
        dl = map(lambda x: os.readlink(x), dl)
        self.sub_devs_cache = set(dl)

        self.log.debug("found sub devs %s held by fdmn %s" % (dl, self.name))
        return self.sub_devs_cache
