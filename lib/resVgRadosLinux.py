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
import resDg
import os
import rcStatus
import rcExceptions as ex
import json
from rcGlobalEnv import *

class Vg(resDg.Dg):
    def __init__(self,
                 rid=None,
                 type=None,
                 pool=None,
                 images=set([]),
                 client_id=None,
                 keyring=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        
        resDg.Dg.__init__(self,
                          rid=rid,
                          type="disk.vg",
                          optional=optional,
                          disabled=disabled,
                          tags=tags,
                          always_on=always_on,
                          monitor=monitor,
                          restart=restart,
                          subset=subset)

        self.pool = pool
        self.images = images
        self.keyring = keyring
        if not client_id.startswith("client."):
            client_id = "client."+client_id
        self.client_id = client_id
        self.label = self.fmt_label()
        self.modprobe_done = False

    def fmt_label(self):
        s = "rados images in pool "+self.pool+": "
        s += ", ".join(self.images)
        return s

    def modprobe(self):
        if self.modprobe_done:
            return
        cmd = ["lsmod"]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.excError("lsmod failed")
        if "rbd" in out.split():
            # no need to load (already loaded or compiled-in)
            return
        cmd = ["modprobe", "rbd"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError("failed to load rbd device driver")
        self.modprobe_done = True

    def showmapped(self, refresh=False):
        if not refresh and hasattr(self, "mapped_data"):
            return self.mapped_data
        self.modprobe()
        cmd = ["rbd", "showmapped", "--format", "json"]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.excError("rbd showmapped failed")
        try:
            _data = json.loads(out)
        except Exception as e:
            raise ex.excError(str(e))
        data = {}
        for id, img_data in _data.items():
            data[(img_data["pool"], img_data["name"])] = img_data
        self.mapped_data = data
        return data

    def rbd_rcmd(self):
        l = ["rbd", "-n", self.client_id, "-p", self.pool]
        if self.keyring:
            l += ["--keyring", self.keyring]
        return l

    def exists(self, image):
        cmd = self.rbd_rcmd()+["info", image]
        ret, out, err = self.call(cmd)
        if ret != 0:
            return False
        return True

    def has_it(self, image):
        mapped = self.showmapped()
        if (self.pool, image) in mapped:
            return True
        return False

    def up_count(self):
        mapped = self.showmapped()
        n = 0
        for image in self.images:
            if (self.pool, image) in mapped:
                n += 1
        return n

    def _status(self, verbose=False):
        n = self.up_count()
        if n == len(self.images):
            if rcEnv.nodename in self.always_on:
                return rcStatus.STDBY_UP
            return rcStatus.UP
        elif n == 0:
            if rcEnv.nodename in self.always_on:
                return rcStatus.STDBY_DOWN
            return rcStatus.DOWN
        else:
            return rcStatus.WARN

    def devname(self, image):
        return os.path.join(os.sep, "dev", "rbd", self.pool, image)

    def do_start_one(self, image):
        mapped = self.showmapped()
        if (self.pool, image) in mapped:
            self.log.info(image+"@"+self.pool+" is already mapped")
            return
        cmd = self.rbd_rcmd()+["map", image]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError("failed to map %s"%self.devname(image))

    def do_start(self):
        for image in self.images:
            self.do_start_one(image)
        self.showmapped(refresh=True)

    def do_stop_one(self, image):
        mapped = self.showmapped()
        if (self.pool, image) not in mapped:
            self.log.info(image+"@"+self.pool+" is already unmapped")
            return
        cmd = ["rbd", "unmap", self.devname(image)]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError("failed to unmap %s"%self.devname(image))

    def do_stop(self):
        for image in self.images:
            self.do_stop_one(image)
        self.showmapped(refresh=True)

    def disklist(self):
        l = set([])
        for image in self.images:
            s = ".".join(("rbd", self.pool, image))
            l.add(s)
        return l

    def devlist(self):
        l = set([])
        for image in self.images:
            s = self.devname(image)
            s = os.path.realpath(s)
            l.add(s)
        return l

    def provision(self):
        m = __import__("provVgRadosLinux")
        prov = getattr(m, "ProvisioningVg")(self)
        prov.provisioner()

class VgLock(Vg):
    def __init__(self,
                 rid=None,
                 type=None,
                 pool=None,
                 images=set([]),
                 client_id=None,
                 keyring=None,
                 lock=None,
                 lock_shared_tag=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        
        self.lock = lock
        self.lock_shared_tag = lock_shared_tag

        Vg.__init__(self,
                    rid=rid,
                    type="disk.vg",
                    pool=pool,
                    images=images,
                    client_id=client_id,
                    keyring=keyring,
                    optional=optional,
                    disabled=disabled,
                    tags=tags,
                    always_on=always_on,
                    monitor=monitor,
                    restart=restart,
                    subset=subset)

        self.label = self.fmt_label()
        self.unlocked = []


    def fmt_label(self):
        return str(self.lock) + " lock on " + Vg.fmt_label(self)

    def locklist(self, image):
        if not self.has_it(image):
            return {}
        cmd = self.rbd_rcmd()+["lock", "list", image, "--format", "json"]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.excError("rbd lock list failed")
        data = {}
        try:
            data = json.loads(out)
        except Exception as e:
            raise ex.excError(str(e))
        return data


    def has_lock(self, image):
        data = self.locklist(image)
        if rcEnv.nodename in data:
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
            if rcEnv.nodename in self.always_on:
                return rcStatus.STDBY_UP
            return rcStatus.UP
        elif n == 0:
            if rcEnv.nodename in self.always_on:
                return rcStatus.STDBY_DOWN
            return rcStatus.DOWN
        else:
            self.rstatus_log("unlocked: "+", ".join(self.unlocked))
            return rcStatus.WARN

    def do_stop_one(self, image):
        data = self.locklist(image)
        if rcEnv.nodename not in data:
            self.log.info(image+"@"+self.pool+" is already unlocked")
            return
        cmd = self.rbd_rcmd()+["lock", "remove", image, rcEnv.nodename, data[rcEnv.nodename]["locker"]]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError("failed to unlock %s: %s"%self.devname(image))

    def do_start_one(self, image):
        data = self.locklist(image)
        if rcEnv.nodename in data:
            self.log.info(image+"@"+self.pool+" is already locked")
            return
        cmd = self.rbd_rcmd()+["lock", "add", image, rcEnv.nodename]
        if self.lock_shared_tag:
            cmd += ["--shared", self.lock_shared_tag]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError("failed to lock %s: %s"%self.devname(image))

    def provision(self):
        return

