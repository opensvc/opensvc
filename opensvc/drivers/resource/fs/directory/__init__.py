import grp
import os
import pwd
import shutil
import stat

import core.exceptions as ex
import core.status
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.files import protected_dir
from utilities.lazy import lazy
from utilities.string import is_string

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "directory"
KEYWORDS = [
    {
        "keyword": "path",
        "at": True, 
        "required": True,
        "text": "The fullpath of the directory to create."
    },
    {
        "keyword": "user",
        "at": True,
        "example": "root",
        "text": "The user that should be owner of the directory. Either in numeric or symbolic form."
    },
    {
        "keyword": "group",
        "at": True,
        "example": "sys",
        "text": "The group that should be owner of the directory. Either in numeric or symbolic form."
    },
    {
        "keyword": "perm",
        "at": True,
        "example": "1777",
        "text": "The permissions the directory should have. A string representing the octal permissions."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class FsDirectory(Resource):
    """Define a mount resource
    """

    def __init__(self,
                 path=None,
                 user=None,
                 group=None,
                 perm=None,
                 zone=None,
                 **kwargs):
        super(FsDirectory, self).__init__(type="fs.directory", **kwargs)
        self.path = path
        self.user = user
        self.group = group
        self.perm = perm
        self.zone = zone

    @lazy
    def mount_point(self):
        return self.path

    def on_add(self):
        if self.zone is None:
            return
        zp = None
        for r in [r for r in self.svc.resources_by_id.values() if r.type == "container.zone"]:
            if r.name == self.zone:
                try:
                    zp = r.zonepath
                except:
                    zp = "<%s>" % self.zone
                break
        if zp is None:
            raise ex.Error("zone %s, referenced in %s, not found" % (self.zone, self.rid))
        self.path = zp + "/root" + self.path
        if "<%s>" % self.zone != zp:
            self.path = os.path.realpath(self.path)
        self.tags.add(self.zone)
        self.tags.add("zone")

    @lazy
    def label(self): # pylint: disable=method-hidden
        if self.path:
            return "dir " + self.path
        else:
            return "dir"

    def start(self):
        self.create()

    def get_gid(self):
        if is_string(self.group):
            try:
                info = grp.getgrnam(self.group)
                self.gid = info[2]
            except KeyError:
                self.gid = None
        else:
            self.gid = int(self.group)

    def get_uid(self):
        if is_string(self.user):
            try:
                info = pwd.getpwnam(self.user)
                self.uid = info[2]
            except KeyError:
                self.uid = None
        else:
            self.uid = int(self.user)

    def create(self):
        if not os.path.exists(self.path):
            self.log.info("create directory %s" % (self.path))
            os.makedirs(self.path)
        if not self.check_uid():
            self.log.info("set %s user to %s" % (self.path, str(self.user)))
            os.chown(self.path, self.uid, -1)
        if not self.check_gid():
            self.log.info("set %s group to %s" % (self.path, str(self.group)))
            os.chown(self.path, -1, self.gid)
        if not self.check_perm():
            self.log.info("set %s perm to %s" % (self.path, str(self.perm)))
            os.chmod(self.path, int(str(self.perm), 8))

    def check_uid(self):
        if self.user is None:
            return True
        if not os.path.exists(self.path):
            return True
        self.get_uid()
        if self.uid is None:
            self.status_log('user %s does not exist' % self.user)
            return True
        uid = os.stat(self.path).st_uid
        if uid != self.uid:
            self.status_log('uid should be %s but is %s'%(str(self.uid), str(uid)))
            return False
        return True

    def check_gid(self):
        if self.group is None:
            return True
        if not os.path.exists(self.path):
            return True
        self.get_gid()
        if self.gid is None:
            self.status_log('group %s does not exist' % self.group)
            return True
        gid = os.stat(self.path).st_gid
        if gid != self.gid:
            self.status_log('gid should be %s but is %s'%(str(self.gid), str(gid)))
            return False
        return True

    def check_perm(self):
        if self.perm is None:
            return True
        if not os.path.exists(self.path):
            return True
        perm = oct(stat.S_IMODE(os.stat(self.path).st_mode))
        perm = str(perm).lstrip("0o").lstrip("0")
        if perm != str(self.perm):
            self.status_log('perm should be %s but is %s'%(str(self.perm), perm))
            return False
        return True

    def _status(self, verbose=False):
        if self.path is None:
            self.status_log("path is not defined", "error")
            return core.status.UNDEF
        if not os.path.exists(self.path):
            self.log.debug("dir %s does not exist" % self.path)
            return core.status.DOWN
        self.check_uid()
        self.check_gid()
        self.check_perm()
        if self.status_logs_count(["warn", "error"]) > 0:
            return core.status.WARN
        else:
            return core.status.NA

    def __str__(self):
        return "%s path=%s user=%s group=%s perm=%s" % (
            super(FsDirectory, self).__str__(),\
            self.path, str(self.user), str(self.group), str(self.perm)
        )

    def __lt__(self, other):
        """
        Order so that deepest mountpoint can be umount first.
        If no ordering constraint, honor the rid order.
        """
        try:
            smnt = os.path.dirname(self.mount_point)
            omnt = os.path.dirname(other.mount_point)
        except AttributeError:
            return self.rid < other.rid
        return (smnt, self.rid) < (omnt, other.rid)

    def provisioned(self):
        return os.path.exists(self.path)

    def provisioner(self):
        pass

    def unprovisioner(self):
        if not os.path.exists(self.path):
            return
        if protected_dir(self.path):
            self.log.warning("cowardly refuse to purge %s", self.path)
        self.log.info("purge %s", self.path)
        shutil.rmtree(self.path)

