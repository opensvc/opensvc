#
# Copyright (c) 2014 Christophe Varoqui <christophe.varoqui@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
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

    def syncupdate(self):
        self.recreate()

    def syncresync(self):
        self.recreate()

    def __init__(self,
                 rid=None,
                 images=[],
                 client_id=None,
                 keyring=None,
                 sync_max_delay=None,
                 sync_interval=None,
                 sync_days=None,
                 sync_period=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type="sync.rados",
                              sync_max_delay=sync_max_delay,
                              sync_interval=sync_interval,
                              sync_days=sync_days,
                              sync_period=sync_period,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        self.label = "snap rados %s"%', '.join(images)
        if len(self.label) > 50:
            self.label = self.label[:46]+"..."
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
            if "/" not in image:
                l.append(image)
        if len(l):
            raise ex.excError("wrong format (expected pool/image): "+", ".join(l))

    def __str__(self):
        return "%s images=%s" % (resSync.Sync.__str__(self),\
                ', '.join(self.images))

