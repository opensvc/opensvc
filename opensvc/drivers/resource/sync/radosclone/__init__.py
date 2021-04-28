import datetime

import core.exceptions as ex
import core.status
from ..radossnap import SyncRadossnap
from core.objects.svcdict import KEYS

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "radosclone"
KEYWORDS = [
    {
        "keyword": "pairs",
        "convert": "list",
        "required": True,
        "at": True,
        "text": "The rados clone device pairs."
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
        data.append("sync.radosclone")
    return data


class SyncRadosclone(SyncRadossnap):
    def __init__(self,
                 type="sync.radosclone",
                 client_id=None,
                 keyring=None,
                 pairs=None,
                 **kwargs):
        pairs = pairs or []
        images = map(lambda x: x.split(":")[0], pairs)
        super(SyncRadosclone, self).__init__(type=type, images=images, **kwargs)
        self.pairs = pairs
        self.fmt_label("clone", pairs)

    def recreate(self):
        self.validate_pair_fmt()
        for pair in self.pairs:
            self._recreate(pair)

    def _recreate(self, pair):
        image, clone = pair.split(":")
        snapnames = self._get_all(image)
        last_date, last_name = self._get_last(image)
        snapname = self.snap_basename() + datetime.datetime.now().strftime(self.date_fmt)
        cmd = self.rbd_cmd()+['snap', 'create', image, '--snap', snapname]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        cmd = self.rbd_cmd()+['snap', 'protect', image+"@"+snapname]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

        list_data = self.list()
        if clone in list_data:
            cmd = self.rbd_cmd()+['rm', clone]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.Error

        cmd = self.rbd_cmd()+['clone', image+"@"+snapname, clone]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

        for snapname in snapnames:
            try:
                self.unprotect(snapname)
                self.rm(snapname)
            except:
                pass

    def validate_pair_fmt(self):
        l = []
        for pair in self.pairs:
            try:
                image, clone = pair.split(":")
            except:
                l.append(image)
                continue
            if image.count("/") != 1 or clone.count("/") != 1:
                l.append(image)
        if len(l) > 0:
            raise ex.Error("wrong format (expected pool/image:pool/image): "+", ".join(l))

    def snap_basename(self):
        return self.rid+".cloneref."

    def _status(self, verbose=False):
        try:
            self.validate_pair_fmt()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN

        try:
            data = self.get_last()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN

        nosnap = []
        noclone = []
        expired = []
        invclone = []
        ok = []

        for image in self.images:
            date, snapname = data[image]

            if date is None:
                nosnap.append(image)
            elif date < datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay):
                expired.append(image)
            else:
                ok.append(image)

        list_data = self.list()
        for pair in self.pairs:
            image, clone = pair.split(":")
            if clone not in list_data:
                noclone.append(pair)
            elif not list_data[clone].get("parent"):
                invclone.append(pair)

        r = core.status.UP

        if len(nosnap) > 0:
            self.status_log("no snap found for images: "+", ".join(nosnap))
            r = core.status.WARN
        if len(expired) > 0:
            self.status_log("snap too old for images: "+", ".join(expired))
            r = core.status.WARN
        if len(noclone) > 0:
            self.status_log("no clone found for pairs: "+", ".join(noclone))
            r = core.status.WARN
        if len(invclone) > 0:
            self.status_log("clone invalid for pairs: "+", ".join(invclone))
            r = core.status.WARN

        return r
