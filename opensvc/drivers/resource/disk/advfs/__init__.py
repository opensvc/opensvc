import json
import os

import core.exceptions as ex
import core.status

from .. import BaseDisk, BASE_KEYWORDS
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "advfs"
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
        "convert": "shlex",
        "provisioning": True,
        "text": "The vgcreate options to use upon vg provisioning."
    },
    {
        "keyword": "pvs",
        "required": True,
        "convert": "list",
        "text": "The list of paths to the physical volumes of the volume group.",
        "provisioning": True
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if not os.path.exists("/etc/fdmns"):
        return data
    if not which("showfdmn"):
        return data
    data.append("disk.advfs")
    return data


class DiskAdvfs(BaseDisk):
    def __init__(self, options=None, pvs=None, **kwargs):
        super(DiskAdvfs, self).__init__(type='disk.advfs', **kwargs)
        self.label = "fdmn %s" % self.name
        self.sub_devs_cache = set()
        self.options = options or []
        self.pvs = pvs or []

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
            if s['overall'].status == core.status.UP:
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
