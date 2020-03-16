import datetime
import json

import rcExceptions as ex
import rcStatus
import resSync
import resSyncRadossnap

from rcUtilities import which, justcall
from svcBuilder import sync_kwargs


def adder(svc, s):
    kwargs = {}
    kwargs["client_id"] = svc.oget(s, "client_id")
    kwargs["keyring"] = svc.oget(s, "keyring")
    kwargs["pairs"] = svc.oget(s, "pairs")
    kwargs.update(sync_kwargs(svc, s))
    r = SyncRadosclone(**kwargs)
    svc += r


class SyncRadosclone(resSyncRadossnap.SyncRadossnap):
    def __init__(self,
                 rid=None,
                 pairs=[],
                 client_id=None,
                 keyring=None,
                 type="sync.radosclone",
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set(),
                 internal=False,
                 subset=None):
        images = map(lambda x: x.split(":")[0], pairs)
        resSyncRadossnap.SyncRadossnap.__init__(self,
                                                rid=rid,
                                                images=images,
                                                client_id=client_id,
                                                keyring=keyring,
                                                type=type,
                                                sync_max_delay=sync_max_delay,
                                                schedule=schedule,
                                                optional=optional,
                                                disabled=disabled,
                                                tags=tags,
                                                subset=subset)

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
        if self.skip_sync(last_date):
            self.log.info("skip resync for image %s: last resync on %s"%(image, str(last_date)))
            return
        snapname = self.snap_basename() + datetime.datetime.now().strftime(self.date_fmt)
        cmd = self.rbd_cmd()+['snap', 'create', image, '--snap', snapname]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        cmd = self.rbd_cmd()+['snap', 'protect', image+"@"+snapname]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

        list_data = self.list()
        if clone in list_data:
            cmd = self.rbd_cmd()+['rm', clone]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.excError

        cmd = self.rbd_cmd()+['clone', image+"@"+snapname, clone]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

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
            raise ex.excError("wrong format (expected pool/image:pool/image): "+", ".join(l))

    def snap_basename(self):
        return self.rid+".cloneref."

    def _status(self, verbose=False):
        try:
            self.validate_pair_fmt()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN

        try:
            data = self.get_last()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN

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

        r = rcStatus.UP

        if len(nosnap) > 0:
            self.status_log("no snap found for images: "+", ".join(nosnap))
            r = rcStatus.WARN
        if len(expired) > 0:
            self.status_log("snap too old for images: "+", ".join(expired))
            r = rcStatus.WARN
        if len(noclone) > 0:
            self.status_log("no clone found for pairs: "+", ".join(noclone))
            r = rcStatus.WARN
        if len(invclone) > 0:
            self.status_log("clone invalid for pairs: "+", ".join(invclone))
            r = rcStatus.WARN

        return r
