import grp
import os
import pwd
import shutil
import stat

import core.exceptions as ex
import core.status
from rcUtilities import lazy, protected_dir
from core.resource import Resource
from core.objects.builder import init_kwargs
from core.objects.svcdict import KEYS
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

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["path"] = svc.oget(s, "path")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["group"] = svc.oget(s, "group")
    kwargs["perm"] = svc.oget(s, "perm")
    zone = svc.oget(s, "zone")

    if zone is not None:
        zp = None
        for r in [r for r in svc.resources_by_id.values() if r.type == "container.zone"]:
            if r.name == zone:
                try:
                    zp = r.zonepath
                except:
                    zp = "<%s>" % zone
                break
        if zp is None:
            svc.log.error("zone %s, referenced in %s, not found"%(zone, s))
            raise ex.Error()
        kwargs["path"] = zp+"/root"+kwargs["path"]
        if "<%s>" % zone != zp:
            kwargs["path"] = os.path.realpath(kwargs["path"])

    r = FsDirectory(**kwargs)

    if zone is not None:
        r.tags.add(zone)
        r.tags.add("zone")

    svc += r


class FsDirectory(Resource):
    """Define a mount resource
    """

    def __init__(self,
                 path=None,
                 user=None,
                 group=None,
                 perm=None,
                 **kwargs):
        super(FsDirectory, self).__init__(type="fs.directory", **kwargs)
        self.path = path
        self.mount_point = path # for fs ordering
        self.user = user
        self.group = group
        self.perm = perm

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
            info = grp.getgrnam(self.group)
            self.gid = info[2]
        else:
            self.gid = int(self.group)

    def get_uid(self):
        if is_string(self.user):
            info = pwd.getpwnam(self.user)
            self.uid = info[2]
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

