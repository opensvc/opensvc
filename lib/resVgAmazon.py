#
# Copyright (c) 2015 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import glob
import time
import rcStatus
import rcExceptions as ex
from rcGlobalEnv import *
from resAmazon import Amazon

class Vg(resDg.Dg, Amazon):
    def __init__(self,
                 rid=None,
                 type="disk.vg",
                 volumes=set([]),
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
                          type=type,
                          optional=optional,
                          disabled=disabled,
                          tags=tags,
                          always_on=always_on,
                          monitor=monitor,
                          restart=restart,
                          subset=subset)

        self.volumes = volumes
        self.label = self.fmt_label()
        self.bdevs = None
        self.mapped_bdevs = None
        self.dev_prefix = None

    def get_bdevs(self, refresh=False):
        if self.bdevs is not None and not refresh:
             return self.bdevs
        data = self.aws(["ec2", "describe-volumes", "--volume-ids"] + self.volumes, verbose=False)
        self.bdevs = [ b["VolumeId"] for b in data["Volumes"] ]
        return self.bdevs

    def get_state(self, vol):
        data = self.aws(["ec2", "describe-volumes", "--volume-ids", vol], verbose=False)
        try:
            avail = data["Volumes"][0]["State"]
        except:
            avail = "not present"
        return avail

    def wait_dev(self, dev):
        dev = self.mangle_devpath(dev)
        for i in range(60):
            if os.path.exists(dev):
                return
            self.log.info("%s is not present yet" % dev)
            time.sleep(1)
        raise ex.excError("timeout waiting for %s to appear." % dev)

    def wait_avail(self, vol):
        for i in range(30):
            state = self.get_state(vol)
            self.log.info("%s state: %s" % (vol, state))
            if state == "available":
                return
            time.sleep(1)
        raise ex.excError("timeout waiting for %s to become available. last %s" % (vol, state))

    def get_mapped_dev(self, volume):
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.excError("can't find instance data")

        devs = []
        for b in instance_data["BlockDeviceMappings"]:
            if b["Ebs"]["VolumeId"] != volume:
                continue
            return b["DeviceName"]

    def get_mapped_devs(self):
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.excError("can't find instance data")

        devs = []
        for b in instance_data["BlockDeviceMappings"]:
            try:
                devs.append(b["DeviceName"])
            except:
                pass
        return devs

    def get_next_dev(self):
        devs = self.get_mapped_devs()
        if (devs) == 0:
            return "/dev/sdb"
        devs = [ r.rstrip("0123456789") for r in devs ]
        devs = [ r.replace("/dev/sd", "") for r in devs ]
        devs += [ r.replace("/dev/sd", "") for r in glob.glob("/dev/sd[a-z]*") ]
        devs += [ r.replace("/dev/xvd", "") for r in glob.glob("/dev/xvd[a-z]*") ]
        chars = "abcdefghijklmnopqrstuvwxyz"
        for c in chars:
            if c not in devs:
                return "/dev/sd"+c
        for c in chars:
            for d in chars:
                if c+d not in devs:
                    return "/dev/sd"+c+d
        for c in chars:
            for d in chars:
                for e in chars:
                    if c+d+e not in devs:
                        return "/dev/sd"+c+d+e
        raise ex.excError("no available device name")

    def get_mapped_bdevs(self, refresh=False):
        if self.mapped_bdevs is not None and not refresh:
             return self.mapped_bdevs
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.excError("can't find instance data")

        self.mapped_bdevs = []
        for b in instance_data["BlockDeviceMappings"]:
            try:
                self.mapped_bdevs.append(b["Ebs"]["VolumeId"])
            except:
                pass
        return self.mapped_bdevs

    def fmt_label(self):
        s = "ec2 volumes "
        s += ", ".join(self.volumes)
        return s

    def has_it(self, volume):
        mapped = self.get_mapped_bdevs()
        if volume in mapped:
            return True
        return False

    def up_count(self):
        mapped = self.get_mapped_bdevs()
        l = []
        for volume in self.volumes:
            if volume in mapped:
                l.append(volume)
        return l

    def validate_volumes(self):
        existing_bdevs = set(self.get_bdevs())
        non_exist = set(self.volumes) - existing_bdevs
        if len(non_exist) > 0:
            raise Exception("non allocated volumes: %s" % ', '.join(non_exist))

    def _status(self, verbose=False):
        try:
            self.validate_volumes()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN
        l = self.up_count()
        n = len(l)
        unmapped = sorted(list(set(self.volumes) - set(l)))
        if n == len(self.volumes):
            if rcEnv.nodename in self.always_on:
                return rcStatus.STDBY_UP
            return rcStatus.UP
        elif n == 0:
            if rcEnv.nodename in self.always_on:
                return rcStatus.STDBY_DOWN
            return rcStatus.DOWN
        else:
            self.status_log("unattached: "+", ".join(unmapped))
            return rcStatus.WARN

    def get_dev_prefix(self):
        if self.dev_prefix is not None:
            return self.dev_prefix
        if len(glob.glob("/dev/xvd*")) > 0:
            self.dev_prefix = "/dev/xvd"
        else:
            self.get_dev_prefix = "/dev/sd"
        return self.dev_prefix

    def mangle_devpath(self, dev):
        return dev.replace("/dev/sd", self.get_dev_prefix())

    def create_static_name(self, dev, vol):
        d = self.create_dev_dir()
        lname = self.rid.replace("#", ".") + "." + str(self.volumes.index(vol))
        l = os.path.join(d, lname)
        s = self.mangle_devpath(dev)
        if os.path.exists(l) and os.path.realpath(l) == s:
            return
        self.log.info("create static device name %s -> %s" % (l, s))
        try:
            os.unlink(l)
        except:
            pass
        os.symlink(s, l)

    def create_dev_dir(self):
        d = os.path.join(rcEnv.pathvar, self.svc.svcname, "dev")
        if os.path.exists(d):
            return d
        os.makedirs(d)
        return d

    def do_start_one(self, volume):
        mapped = self.get_mapped_bdevs()
        if volume in mapped:
            self.log.info(volume+" is already attached")
            dev = self.get_mapped_dev(volume)
        else:
            dev = self.get_next_dev()
            data = self.aws([
              "ec2", "attach-volume",
              "--instance-id", self.get_instance_id(),
              "--volume-id", volume,
              "--device", dev
            ])
        self.wait_dev(dev)
        self.create_static_name(dev, volume)

    def do_start(self):
        self.validate_volumes()
        for volume in self.volumes:
            self.do_start_one(volume)
            self.can_rollback = True
        self.get_mapped_bdevs(refresh=True)

    def do_stop_one(self, volume):
        mapped = self.get_mapped_bdevs()
        if volume not in mapped:
            self.log.info(volume+" is already detached")
            return
        data = self.aws([
          "ec2", "detach-volume",
          "--instance-id", self.get_instance_id(),
          "--volume-id", volume
        ])

    def do_stop(self):
        self.validate_volumes()
        for volume in self.volumes:
            self.do_stop_one(volume)
        self.get_mapped_bdevs(refresh=True)

    def devlist(self):
        devs = self.get_mapped_devs()
        if len(devs) == 0:
            return devs
        if not os.path.exists(devs[0]):
            return set([ r.replace("/dev/sd", "/dev/xvd") for r in devs ])
        return devs

    def disklist(self):
        disks = set([ r.rstrip("1234567890") for r in self.devlist() ])
        return disks

    def provision(self):
        m = __import__("provVgAmazon")
        prov = getattr(m, "ProvisioningVg")(self)
        prov.provisioner()

