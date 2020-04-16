import json
import os

import core.status
import core.exceptions as ex

from .. import BaseDisk, BASE_KEYWORDS
from utilities.converters import convert_size
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "rados"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "client_id",
        "text": "Client id to use for authentication with the rados servers."
    },
    {
        "keyword": "keyring",
        "text": "keyring to look for the client id secret for authentication with the rados servers."
    },
    {
        "keyword": "lock",
        "candidates": ["exclusive", "shared", "None"],
        "text": "Locking mode for the rados images"
    },
    {
        "keyword": "lock_shared_tag",
        "depends": [('lock', ['shared'])],
        "text": "The tag to use upon rados image locking in shared mode"
    },
    {
        "keyword": "image_format",
        "provisioning": True,
        "default": "2",
        "text": "The rados image format"
    },
    {
        "keyword": "size",
        "convert": "size",
        "provisioning": True,
        "text": "The block device size in size expression format."
    },
    {
        "keyword": "images",
        "convert": "list",
        "required": True,
        "text": "The rados image names handled by this vg resource. whitespace separated."
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
    if which("rbd"):
        return ["disk.rados"]
    return []


class DiskRados(BaseDisk):
    def __init__(self,
                 type="disk.rados",
                 images=None,
                 client_id=None,
                 keyring=None,
                 lock_shared_tag=None,
                 lock=None,
                 image_format=None,
                 size=None,
                 **kwargs):
        super(DiskRados, self).__init__(type=type, **kwargs)
        self.images = images or set()
        self.keyring = keyring
        if not client_id.startswith("client."):
            client_id = "client."+client_id
        self.client_id = client_id
        self.image_format = image_format
        self.size = size
        self.lock = lock
        self.lock_shared_tag = lock_shared_tag
        self.label = self.fmt_label()
        self.modprobe_done = False

    def on_add(self):
        if self.lock:
            kwargs = {
                "rid": self.rid + "lock",
                "lock": self.lock,
                "lock_shared_tag": self.lock_shared_tag,
            }
            r = DiskRadoslock(**kwargs)
            self.svc += r

    def validate_image_fmt(self):
        l = []
        for image in self.images:
            if "/" not in image:
                l.append(image)
        if len(l):
            raise ex.Error("wrong format (expected pool/image): "+", ".join(l))

    def fmt_label(self):
        s = "rados images: "
        s += ", ".join(self.images)
        return s

    def modprobe(self):
        if self.modprobe_done:
            return
        cmd = [Env.syspaths.lsmod]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.Error("lsmod failed")
        if "rbd" in out.split():
            # no need to load (already loaded or compiled-in)
            return
        cmd = ["modprobe", "rbd"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("failed to load rbd device driver")
        self.modprobe_done = True

    def showmapped(self, refresh=False):
        if not refresh:
            try:
                return getattr(self, "mapped_data")
            except AttributeError:
                pass
        self.modprobe()
        cmd = ["rbd", "showmapped", "--format", "json"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("rbd showmapped failed: "+err)
        try:
            _data = json.loads(out)
        except Exception as e:
            raise ex.Error(str(e))
        data = {}
        for id, img_data in _data.items():
            data[img_data["pool"]+"/"+img_data["name"]] = img_data
        self.mapped_data = data
        return data

    def rbd_rcmd(self):
        l = ["rbd", "-n", self.client_id]
        if self.keyring:
            l += ["--keyring", self.keyring]
        return l

    def exists(self, image):
        cmd = self.rbd_rcmd()+["info", image]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        return True

    def has_it(self, image):
        mapped = self.showmapped()
        if image in mapped:
            return True
        return False

    def up_count(self):
        mapped = self.showmapped()
        l = []
        for image in self.images:
            if image in mapped:
                l.append(image)
        return l

    def _status(self, verbose=False):
        try:
            self.validate_image_fmt()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN
        l = self.up_count()
        n = len(l)
        unmapped = sorted(list(set(self.images) - set(l)))
        if n == len(self.images):
            return core.status.UP
        elif n == 0:
            return core.status.DOWN
        else:
            self.status_log("unmapped: "+", ".join(unmapped))
            return core.status.DOWN

    def devname(self, image):
        return os.path.join(os.sep, "dev", "rbd", image)

    def do_start_one(self, image):
        mapped = self.showmapped()
        if image in mapped:
            self.log.info(image+" is already mapped")
            return
        cmd = self.rbd_rcmd()+["map", image]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("failed to map %s"%self.devname(image))

    def do_start(self):
        self.validate_image_fmt()
        for image in self.images:
            self.do_start_one(image)
            self.can_rollback = True
        self.showmapped(refresh=True)

    def do_stop_one(self, image):
        mapped = self.showmapped()
        if image not in mapped:
            self.log.info(image+" is already unmapped")
            return
        cmd = ["rbd", "unmap", self.devname(image)]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("failed to unmap %s"%self.devname(image))


    def do_stop(self):
        self.validate_image_fmt()
        for image in self.images:
            self.do_stop_one(image)
        self.showmapped(refresh=True)

    def exposed_disks(self):
        l = set()
        for image in self.images:
            s = ".".join(("rbd", image.replace("/", ".")))
            l.add(s)
        return l

    def exposed_devs(self):
        l = set()
        for image in self.images:
            s = self.devname(image)
            s = os.path.realpath(s)
            l.add(s)
        return l

class DiskRadoslock(DiskRados):
    def __init__(self, lock=None, lock_shared_tag=None, **kwargs):
        super(DiskRadoslock, self).__init__(type="disk.radoslock", **kwargs)
        self.lock = lock
        self.lock_shared_tag = lock_shared_tag
        self.label = self.fmt_label()
        self.unlocked = []

    def fmt_label(self):
        return "%s lock on %s" % (
            self.lock,
            super(DiskRadoslock, self).fmt_label()
        )

    def locklist(self, image):
        cmd = self.rbd_rcmd()+["lock", "list", image, "--format", "json"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("rbd lock list failed")
        data = {}
        try:
            data = json.loads(out)
        except Exception as e:
            raise ex.Error(str(e))
        return data


    def has_lock(self, image):
        data = self.locklist(image)
        if Env.nodename in data:
            return True
        self.unlocked.append(image)
        return False

    def up_count(self):
        n = 0
        for image in self.images:
            if self.has_lock(image):
                n += 1
        return n

    def _status(self, verbose=False):
        n = self.up_count()
        if n == len(self.images):
            return core.status.UP
        elif n == 0:
            return core.status.DOWN
        else:
            self.status_log("unlocked: "+", ".join(self.unlocked))
            return core.status.DOWN

    def do_stop_one(self, image):
        data = self.locklist(image)
        if Env.nodename not in data:
            self.log.info(image+" is already unlocked")
            return
        i = 0
        while len(data) > 0 or i>20:
            i += 1
            cmd = self.rbd_rcmd()+["lock", "remove", image, Env.nodename, data[Env.nodename]["locker"]]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.Error("failed to unlock %s"%self.devname(image))
            data = self.locklist(image)

    def do_start_one(self, image):
        data = self.locklist(image)
        if Env.nodename in data:
            self.log.info(image+" is already locked")
            return
        cmd = self.rbd_rcmd()+["lock", "add", image, Env.nodename]
        if self.lock_shared_tag:
            cmd += ["--shared", self.lock_shared_tag]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("failed to lock %s"%self.devname(image))

    def provisioner(self):
        for image in self.images:
            self.provisioner_one(image)
        self.log.info("provisioned")
        self.start()
        self.svc.node.unset_lazy("devtree")

    def provisioner_one(self, image):
        if self.exists(image):
            self.log.info("%s already provisioned"%image)
            return
        size = convert_size(self.size, _to="m")
        cmd = self.rbd_rcmd() + ['create', '--size', str(size), image]
        if self.image_format:
            cmd += ["--image-format", str(self.image_format)]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.svc.node.unset_lazy("devtree")

