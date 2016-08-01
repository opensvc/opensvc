import resDg
import os
import json
import rcStatus
import rcExceptions as ex
from rcGlobalEnv import *
from rcUtilities import justcall

class Vg(resDg.Dg):
    def __init__(self,
                 rid=None,
                 type="disk.gce",
                 names=set([]),
                 gce_zone=None,
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

        self.names = names
        self.gce_zone = gce_zone
        self.label = self.fmt_label()

    def get_disk_names(self, refresh=False):
        data = self.get_disks(refresh=refresh)
        return [d["name"] for d in data]

    def get_attached_disk_names(self, refresh=False):
        data = self.get_attached_disks(refresh=refresh)
        return [d["name"] for d in data]

    def get_attached_disks(self, refresh=False):
        if hasattr(self.svc, "gce_attached_disks") and not refresh:
             return self.svc.gce_attached_disks
        cmd = ["gcloud", "compute", "instances", "describe", rcEnv.nodename, "--format", "json", "--zone", self.gce_zone]
        out, err, ret = justcall(cmd)
        data = json.loads(out)
        data = data.get("disks", [])
        for i, d in enumerate(data):
            data[i]["name"] = d["source"].split("/")[-1]
        self.svc.gce_attached_disks = data
        return self.svc.gce_attached_disks

    def get_disks(self, refresh=False):
        if hasattr(self.svc, "gce_disks") and not refresh:
             return self.svc.gce_disks
        cmd = ["gcloud", "compute", "disks", "list", "--format", "json", "--zone", self.gce_zone]
        out, err, ret = justcall(cmd)
        data = json.loads(out)
        self.svc.gce_disks = data
        return data

    def fmt_label(self):
        s = "gce volumes "
        s += ", ".join(self.names)
        return s

    def has_it(self, name):
        data = self.get_attached_disks()
        disk_names = [d.get("name") for d in data]
        if name in disk_names:
            return True
        return False

    def up_count(self):
        data = self.get_attached_disks()
        disk_names = [d.get("name") for d in data]
        l = []
        for name in self.names:
            if name in disk_names:
                l.append(name)
        return l

    def validate_volumes(self):
        existing = [d.get("name") for d in self.get_disks()]
        non_exist = set(self.names) - set(existing)
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
        unattached = sorted(list(set(self.names) - set(l)))
        if n == len(self.names):
            if rcEnv.nodename in self.always_on:
                return rcStatus.STDBY_UP
            return rcStatus.UP
        elif n == 0:
            if rcEnv.nodename in self.always_on:
                return rcStatus.STDBY_DOWN
            return rcStatus.DOWN
        else:
            self.status_log("unattached: "+", ".join(unattached))
            return rcStatus.WARN

    def detach_other(self, name):
        existing = self.get_disks()
        for d in existing:
            if d["name"] != name:
                continue
            for user in d.get("users", []):
                instance = user.split('/')[-1]
                if instance != rcEnv.nodename:
                    self.vcall([
                      "gcloud", "compute", "instances", "detach-disk", "-q",
                      instance,
                      "--disk", name, 
                      "--zone", self.gce_zone
                    ])

    def do_start_one(self, name):
        existing = self.get_disk_names()
        if name not in existing:
            self.log.info(name+" does not exist")
            return
        attached = self.get_attached_disk_names()
        if name in attached:
            self.log.info(name+" is already attached")
            return

        self.detach_other(name)
        self.vcall([
          "gcloud", "compute", "instances", "attach-disk", "-q",
          rcEnv.nodename,
          "--disk", name, 
          "--zone", self.gce_zone,
          "--device-name", self.fmt_disk_devname(name),
        ])
        self.can_rollback = True

    def do_start(self):
        for name in self.names:
            self.do_start_one(name)
        self.get_attached_disks(refresh=True)

    def do_stop_one(self, name):
        existing = self.get_disk_names()
        if name not in existing:
            self.log.info(name+" does not exist")
            return
        attached = self.get_attached_disk_names()
        if name not in attached:
            self.log.info(name+" is already detached")
            return
        self.vcall([
          "gcloud", "compute", "instances", "detach-disk", "-q",
          rcEnv.nodename,
          "--disk", name,
          "--zone", self.gce_zone
        ])

    def do_stop(self):
        for name in self.names:
            self.do_stop_one(name)
        self.get_attached_disks(refresh=True)

    def fmt_disk_devname(self, name):
        index = self.names.index(name)
        return ".".join([self.svc.svcname, self.rid.replace("#", "."), str(index)])

    def devlist(self):
        attached = self.get_attached_disks()
        return set(["/dev/disk/by-id/google-"+d["deviceName"] for d in attached if d["name"] in self.names])

    def disklist(self):
        attached = self.get_attached_disks()
        return set([d["deviceName"] for d in attached if d["name"] in self.names])

    def provision(self):
        m = __import__("provVgGce")
        prov = getattr(m, "ProvisioningVg")(self)
        prov.provisioner()

