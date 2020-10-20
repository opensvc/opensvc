import datetime
import json

import core.exceptions as ex
import core.status
from .. import Sync, notify
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "radossnap"
KEYWORDS = [
    {
        "keyword": "images",
        "convert": "list",
        "required": True,
        "text": "The rados image names handled by this sync resource. whitespace separated."
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
    if which("rbd"):
        data.append("sync.radossnap")
    return data


class SyncRadossnap(Sync):
    def __init__(self,
                 images=None,
                 client_id=None,
                 keyring=None,
                 **kwargs):
        super(SyncRadossnap, self).__init__(type="sync.radossnap", **kwargs)

        if images is None:
            images = []
        self.fmt_label("snap", images)
        self.images = images
        if not client_id.startswith("client."):
            client_id = "client."+client_id
        self.client_id = client_id
        self.keyring = keyring
        self.list_data = None
        self.date_fmt = "%Y-%m-%d.%H:%M:%S"

    def __str__(self):
        return "%s images=%s" % (
            super(SyncRadossnap, self).__str__(),
            ", ".join(self.images)
        )

    def recreate(self):
        self.validate_image_fmt()
        for image in self.images:
            self._recreate(image)

    def _recreate(self, image):
        snapnames = self._get_all(image)
        last_date, last_name = self._get_last(image)
        snapname = self.snap_basename() + datetime.datetime.now().strftime(self.date_fmt)
        cmd = self.rbd_cmd()+['snap', 'create', image, '--snap', snapname]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

        for snapname in snapnames:
            self.rm(snapname)

    def rm(self, image):
        cmd = self.rbd_cmd()+['snap', 'rm', image]
        ret, out, err = self.vcall(cmd)

    def unprotect(self, image):
        cmd = self.rbd_cmd()+['snap', 'unprotect', image]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def get_all(self):
        data = {}
        for image in self.images:
            data[image] = self._get_all(image)
        return data

    def _get_all(self, image):
        data = self.list()
        retained = []
        prefix = image+"@"+self.snap_basename()
        for name in data:
            if not name.startswith(prefix):
                continue
            retained.append(name)
        return retained

    def get_last(self):
        data = {}
        for image in self.images:
            data[image] = self._get_last(image)
        return data

    def _get_last(self, image):
        data = self.list()
        retained = []
        prefix = image+"@"+self.snap_basename()
        for name in data:
            if not name.startswith(prefix):
                continue
            try:
                date = datetime.datetime.strptime(name, prefix+self.date_fmt)
            except:
                continue
            retained.append((date, name))
        if len(retained) == 0:
            return None, None
        last_date, last_name = sorted(retained)[-1]
        return last_date, last_name

    def rbd_cmd(self):
        l = ["rbd"]
        if self.client_id:
            l += ["-n", self.client_id]
        if self.keyring:
            l += ["--keyring", self.keyring]
        return l

    def snap_basename(self):
        return self.rid+"."

    def get_pools(self):
        l = set()
        for image in self.images:
            pool = image.split("/")[0]
            l.add(pool)
        return l

    def list(self):
        if self.list_data is not None:
            return self.list_data
        data = {}
        for pool in self.get_pools():
            data.update(self._list(pool))

        self.list_data = data
        return data

    def _list(self, pool):
        cmd = self.rbd_cmd() + ["ls", "-l", pool, "--format", "json"]
        out, err, ret = justcall(cmd)
        data = {}
        try:
            _data = json.loads(out)
        except Exception as e:
            self.status_log(str(e))
            _data = []
        for img_data in _data:
            idx = pool+"/"+img_data['image']
            if "snapshot" in img_data:
                idx += "@"+img_data['snapshot']
            data[idx] = img_data
        return data

    def sync_status(self, verbose=False):
        try:
            self.validate_image_fmt()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN

        try:
            data = self.get_last()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN

        nosnap = []
        expired = []
        ok = []

        for image in self.images:
            date, snapname = data[image]

            if date is None:
                nosnap.append(image)
            elif date < datetime.datetime.now() - datetime.timedelta(seconds=self.sync_max_delay):
                expired.append(image)
            else:
                ok.append(image)

        r = core.status.UP

        if len(nosnap) > 0:
            self.status_log("no snap found for images: "+", ".join(nosnap))
            r = core.status.WARN
        if len(expired) > 0:
            self.status_log("snap too old for images: "+", ".join(expired))
            r = core.status.WARN

        return r

    @notify
    def sync_update(self):
        self.recreate()

    def sync_resync(self):
        self.recreate()

    def validate_image_fmt(self):
        l = []
        for image in self.images:
            if image.count("/") != 1:
                l.append(image)
        if len(l) > 0:
            raise ex.Error("wrong format (expected pool/image): "+", ".join(l))

    def fmt_label(self, t, l):
        self.label = t+" rados %s"%', '.join(l)
        if len(self.label) > 80:
            self.label = self.label[:76]+"..."
