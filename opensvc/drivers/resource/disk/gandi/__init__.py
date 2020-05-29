import grp
import os
import pwd
import stat

import core.status
import core.exceptions as ex

from .. import BaseDisk, BASE_KEYWORDS
from env import Env
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.string import is_string

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "gandi"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "node",
        "at": True,
        "default_text": "The local node name.",
        "text": "The node name from the Gandi api point of view.",
    },
    {
        "keyword": "user",
        "at": True,
        "example": "root",
        "text": "The user that should be owner of the device. Either in numeric or symbolic form."
    },
    {
        "keyword": "group",
        "at": True,
        "example": "sys",
        "text": "The group that should be owner of the device. Either in numeric or symbolic form."
    },
    {
        "keyword": "perm",
        "at": True,
        "example": "600",
        "text": "The permissions the device should have. A string representing the octal permissions."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    try:
        from libcloud.compute.types import Provider
        from libcloud.compute.providers import get_driver
        return ["disk.gandi"]
    except ImportError:
        return []

class DiskGandi(BaseDisk):
    def __init__(self,
                 node=None,
                 cloud_id=None,
                 user="root",
                 group="root",
                 perm="660",
                 **kwargs):
        super(DiskGandi, self).__init__(type='disk.gandi', **kwargs)
        self.label = "gandi volume %s" % self.name
        self.node = node
        self.cloud_id = cloud_id
        self.user = user
        self.group = group
        self.perm = perm

        self.get_uid()
        self.get_gid()

    def print_obj(self, n):
        for k in dir(n):
            if '__' in k:
                continue
            print(k, "=", getattr(n, k))

    @lazy
    def cloud(self):
        try:
            cloud = self.svc.node.cloud_get(self.cloud_id)
        except ex.InitError as e:
            raise ex.Error(str(e))
        return cloud

    def get_uid(self):
        self.uid = self.user
        if is_string(self.uid):
            try:
                info=pwd.getpwnam(self.uid)
                self.uid = info[2]
            except:
                pass

    def get_gid(self):
        self.gid = self.group
        if is_string(self.gid):
            try:
                info=grp.getgrnam(self.gid)
                self.gid = info[2]
            except:
                pass

    def check_uid(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        uid = os.stat(rdev).st_uid
        if uid != self.uid:
            if verbose:
                self.status_log('%s uid should be %d but is %d'%(rdev, self.uid, uid))
            return False
        return True

    def check_gid(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        gid = os.stat(rdev).st_gid
        if gid != self.gid:
            if verbose:
                self.status_log('%s gid should be %d but is %d'%(rdev, self.gid, gid))
            return False
        return True

    def check_perm(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        try:
            perm = oct(stat.S_IMODE(os.stat(rdev).st_mode))
        except:
            self.log.error('%s can not stat file'%rdev)
            return False
        perm = str(perm).lstrip("0o").lstrip("0")
        if perm != str(self.perm):
            if verbose:
                self.status_log('%s perm should be %s but is %s'%(rdev, str(self.perm), perm))
            return False
        return True

    def has_it(self):
        """Returns True if all devices are present
        """
        try:
            node = self.get_node()
        except ex.Error as e:
            raise ex.Error("can't find cloud node to list volumes (%s)"%str(e))

        disks = self.cloud.driver._node_info(node.id)['disks']
        for disk in disks:
            if disk['name'] == self.name:
                return True
        return False

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        return self.has_it()

    def _status(self, verbose=False):
        try:
            s = self.is_up()
        except ex.Error as e:
            self.status_log(str(e))
            return core.status.WARN
        if s:
            return core.status.UP
        else:
            return core.status.DOWN

    def get_node(self):
        if self.node is not None:
            n = self.node
        else:
            n = Env.nodename
        try:
            nodes = self.cloud.driver.list_nodes()
        except Exception as e:
            raise ex.Error(str(e))
        for node in nodes:
            if node.name == n:
                return node
        raise ex.Error()

    def get_disk(self):
        disks = self.cloud.driver.ex_list_disks()
        _disk = None
        for disk in disks:
            if disk.name == self.name:
                _disk = disk
        if _disk is None:
            raise ex.Error()
        return _disk

    def do_start(self):
        try:
            node = self.get_node()
        except ex.Error as e:
            raise ex.Error("can't find cloud node to attach volume %s to (%s)"%(self.name, str(e)))

        try:
            disk = self.get_disk()
        except:
            raise ex.Error("volume %s not found in %s"%(self.name, self.cloud_id))

        try:
            status = self.is_up()
        except ex.Error as e:
            self.log.error("abort gandi volume %s attach: %s"%(self.name, str(e)))

        if status:
            self.log.info("gandi volume %s is already attached"%self.name)
            return

        self.log.info("attach gandi volume %s"%self.name)
        self.cloud.driver.ex_node_attach_disk(node, disk)
        self.can_rollback = True

    def do_stop(self):
        try:
            node = self.get_node()
        except ex.Error as e:
            raise ex.Error("can't find cloud node to detach volume %s from: %s"%(self.name, str(e)))

        try:
            disk = self.get_disk()
        except:
            raise ex.Error("volume %s not found in %s"%(self.name, self.cloud_id))

        try:
            status = self.is_up()
        except ex.Error as e:
            self.log.error("abort gandi volume %s detach: %s"%(self.name, str(e)))

        if not status:
            self.log.info("gandi volume %s is already detached"%self.name)
            return

        self.log.info("detach gandi volume %s"%self.name)
        self.cloud.driver.ex_node_detach_disk(node, disk)

    def shutdown(self):
        pass

