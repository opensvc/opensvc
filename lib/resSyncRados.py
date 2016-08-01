import json

from rcUtilities import which, justcall
import rcExceptions as ex
import rcStatus
import datetime
import resSync

class syncRadosSnap(resSync.Sync):
    def recreate(self):
        self.validate_image_fmt()
        for image in self.images:
            self._recreate(image)

    def _recreate(self, image):
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

        for snapname in snapnames:
            self.rm(snapname)

    def rm(self, image):
        cmd = self.rbd_cmd()+['snap', 'rm', image]
        ret, out, err = self.vcall(cmd)

    def unprotect(self, image):
        cmd = self.rbd_cmd()+['snap', 'unprotect', image]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

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
        l = set([])
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

    def _status(self, verbose=False):
        try:
            self.validate_image_fmt()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN

        try:
            data = self.get_last()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN

        nosnap = []
        expired = []
        ok = []

        for image in self.images:
            date, snapname = data[image]

            if date is None:
                nosnap.append(image)
            elif date < datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay):
                expired.append(image)
            else:
                ok.append(image)

        r = rcStatus.UP

        if len(nosnap) > 0:
            self.status_log("no snap found for images: "+", ".join(nosnap))
            r = rcStatus.WARN
        if len(expired) > 0:
            self.status_log("snap too old for images: "+", ".join(expired))
            r = rcStatus.WARN

        return r

    def sync_update(self):
        self.recreate()

    def sync_resync(self):
        self.recreate()

    def __init__(self,
                 rid=None,
                 images=[],
                 client_id=None,
                 keyring=None,
                 type="sync.rados",
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type=type,
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        self.fmt_label("snap", images)
        self.images = images
        if not client_id.startswith("client."):
            client_id = "client."+client_id
        self.client_id = client_id
        self.keyring = keyring
        self.list_data = None
        self.date_fmt = "%Y-%m-%d.%H:%M:%S"

    def validate_image_fmt(self):
        l = []
        for image in self.images:
            if image.count("/") != 1:
                l.append(image)
        if len(l) > 0:
            raise ex.excError("wrong format (expected pool/image): "+", ".join(l))

    def fmt_label(self, t, l):
        self.label = t+" rados %s"%', '.join(l)
        if len(self.label) > 80:
            self.label = self.label[:76]+"..."

    def __str__(self):
        return "%s images=%s" % (resSync.Sync.__str__(self),\
                ', '.join(self.images))

class syncRadosClone(syncRadosSnap):
    def __init__(self,
                 rid=None,
                 pairs=[],
                 client_id=None,
                 keyring=None,
                 type="sync.rados",
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        images = map(lambda x: x.split(":")[0], pairs)
        syncRadosSnap.__init__(self,
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
