import json

import core.status
from .. import BaseDisk, BASE_KEYWORDS
from utilities.converters import convert_size
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import justcall
from utilities.subsystems.gce import GceMixin

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "gce"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "names",
        "convert": "list",
        "at": True,
        "required": True,
        "text": "Set the gce disk names",
        "example": "svc1-disk1"
    },
    {
        "keyword": "gce_zone",
        "at": True,
        "required": True,
        "text": "Set the gce zone",
        "example": "europe-west1-b"
    },
    {
        "keyword": "description",
        "provisioning": True,
        "at": True,
        "text": "An optional, textual description for the disks being created.",
        "example": "foo"
    },
    {
        "keyword": "image",
        "provisioning": True,
        "at": True,
        "text": "An image to apply to the disks being created. When using this option, the size of the disks must be at least as large as the image size.",
        "example": "centos-7"
    },
    {
        "keyword": "image_project",
        "provisioning": True,
        "at": True,
        "text": "The project against which all image references will be resolved.",
        "example": "myprj"
    },
    {
        "keyword": "size",
        "provisioning": True,
        "at": True,
        "convert": "size",
        "text": "A size expression for the disk allocation.",
        "example": "20g"
    },
    {
        "keyword": "source_snapshot",
        "provisioning": True,
        "at": True,
        "text": "A source snapshot used to create the disks. It is safe to delete a snapshot after a disk has been created from the snapshot. In such cases, the disks will no longer reference the deleted snapshot. When using this option, the size of the disks must be at least as large as the snapshot size.",
        "example": "mysrcsnap"
    },
    {
        "keyword": "disk_type",
        "provisioning": True,
        "at": True,
        "text": "Specifies the type of disk to create. To get a list of available disk types, run :cmd:`gcloud compute disk-types list`. The default disk type is ``pd-standard``.",
        "example": "pd-standard"
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
    if which("gcloud"):
        return ["disk.gce"]
    return []


class DiskGce(BaseDisk, GceMixin):
    def __init__(self, names=None, gce_zone=None, description=None, image=None, image_project=None, size=None, source_snapshot=None, disk_type=None, **kwargs):
        BaseDisk.__init__(self, type="disk.gce", **kwargs)
        self.names = names or set()
        self.gce_zone = gce_zone
        self.description = description
        self.image = image
        self.image_project = image_project
        self.size = size
        self.source_snapshot = source_snapshot
        self.disk_type = disk_type
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
        self.wait_gce_auth()
        cmd = ["gcloud", "compute", "instances", "describe", Env.nodename, "--format", "json", "--zone", self.gce_zone]
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
        self.wait_gce_auth()
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
            return core.status.WARN
        l = self.up_count()
        n = len(l)
        unattached = sorted(list(set(self.names) - set(l)))
        if n == len(self.names):
            return core.status.UP
        elif n == 0:
            return core.status.DOWN
        else:
            self.status_log("unattached: "+", ".join(unattached))
            return core.status.DOWN

    def detach_other(self, name):
        existing = self.get_disks()
        for d in existing:
            if d["name"] != name:
                continue
            for user in d.get("users", []):
                instance = user.split('/')[-1]
                if instance != Env.nodename:
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
          Env.nodename,
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
          Env.nodename,
          "--disk", name,
          "--zone", self.gce_zone
        ])

    def do_stop(self):
        for name in self.names:
            self.do_stop_one(name)
        self.get_attached_disks(refresh=True)

    def fmt_disk_devname(self, name):
        index = self.names.index(name)
        if self.svc.namespace:
            return ".".join([self.svc.namespace.lower(), self.svc.name, self.rid.replace("#", "."), str(index)])
        else:
            return ".".join([self.svc.name, self.rid.replace("#", "."), str(index)])

    def exposed_devs(self):
        attached = self.get_attached_disks()
        return set(["/dev/disk/by-id/google-"+d["deviceName"] for d in attached if d["name"] in self.names])

    def exposed_disks(self):
        attached = self.get_attached_disks()
        return set([d["deviceName"] for d in attached if d["name"] in self.names])


    def provisioner(self):
        for name in self.names:
            self._provisioner(name)
        self.log.info("provisioned")
        self.get_disks(refresh=True)
        self.start()
        self.svc.node.unset_lazy("devtree")

    def _provisioner(self, name):
        disk_names = self.get_disk_names()
        if name in disk_names:
            self.log.info("gce disk name %s already provisioned" % name)
            return

        size = str(convert_size(self.size, _to="MB"))+'MB'

        cmd = ["gcloud", "compute", "disks", "create", "-q",
               name,
               "--size", size,
               "--zone", self.gce_zone]

        if self.description:
            cmd += ["--description", self.description]
        if self.image:
            cmd += ["--image", self.image]
        if self.source_snapshot:
            cmd += ["--source-snapshot", self.source_snapshot]
        if self.image_project:
            cmd += ["--image-project", self.image_project]
        if self.disk_type:
            cmd += ["--type", self.disk_type]

        self.vcall(cmd)

    def unprovisioner(self):
        self.stop()
        for name in self.names:
            self._unprovisioner(name)
        self.log.info("unprovisioned")
        self.svc.node.unset_lazy("devtree")

    def _unprovisioner(self, name):
        disk_names = self.get_disk_names()
        if name not in disk_names:
            self.log.info("gce disk name %s already unprovisioned" % name)
            return

        cmd = ["gcloud", "compute", "disks", "delete", "-q", name,
               "--zone", self.gce_zone]

        self.vcall(cmd)


