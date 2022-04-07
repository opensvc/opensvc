import os
import re

import core.exceptions as ex
import utilities.devices.linux

from .. import BaseDisk, BASE_KEYWORDS
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import justcall, which
from utilities.lazy import lazy
from utilities.naming import factory


DRIVER_GROUP = "disk"
DRIVER_BASENAME = "crypt"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "name",
        "at": True,
        "text": "The basename of the exposed device.",
        "default_text": "The basename of the underlying device, suffixed with '-crypt'.",
        "example": "{fqdn}-crypt"
    },
    {
        "keyword": "dev",
        "at": True,
        "required": True,
        "text": "The fullpath of the underlying block device.",
        "example": "/dev/{fqdn}/lv1"
    },
    {
        "keyword": "manage_passphrase",
        "at": True,
        "convert": "boolean",
        "text": "By default, on provision the driver allocates a new random passphrase (256 printable chars), and forgets it on unprovision. If set to false, require a passphrase to be already present in the sec object to provision, and don't remove it on unprovision.",
        "provisioning": True,
        "default": True,
    },
    {
        "keyword": "secret",
        "at": True,
        "text": "The name of the sec object hosting the crypt secrets. The sec object must be in the same namespace than the object defining the disk.crypt resource.",
        "default": "{name}",
    },
    {
        "keyword": "label",
        "at": True,
        "text": "The label to set in the cryptsetup metadata writen on dev. A label helps admin understand the role of a device.",
        "default": "{fqdn}",
        "provisioning": True,
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
    if which("cryptsetup"):
        return ["disk.crypt"]
    return []

def gen_passphrase():
    import string
    import random
    characters = string.ascii_letters + string.digits + '!"#$%&()*+,-./:;<=>?@[]^_`{|}~'
    return "".join(random.choice(characters) for i in range(256))

class DiskCrypt(BaseDisk):
    def __init__(self,
                 name=None,
                 dev=None,
                 secret=None,
                 label=None,
                 manage_passphrase=True,
                 **kwargs):
        super(DiskCrypt, self).__init__(type='disk.crypt', **kwargs)
        self.dev = dev
        self.name = name
        self.secret = secret
        self.fmtlabel = label
        self.manage_passphrase = manage_passphrase

    @lazy
    def label(self):  # pylint: disable=method-hidden
        return "crypt %s" % self.exposed_dev()

    def _info(self):
        name = self.get_name()
        dev = self.get_dev()
        data = [
          ["name", name],
          ["dev", dev],
          ["secret", self.secret],
          ["label", self.fmtlabel],
          ["manage_passphrase", self.manage_passphrase],
        ]
        return data

    def sec(self):
        if not self.secret:
            return
        return factory("sec")(self.secret, namespace=self.svc.namespace, node=self.svc.node)

    def passphrase_key(self):
        key = self.rid.replace("#", "_")
        key += "_crypt_passphrase"
        return key

    def forget_passphrase(self):
        sec = self.sec()
        key = self.passphrase_key()
        if not self.manage_passphrase:
            self.log.info("leave key %s in %s", key, sec.path)
            return
        self.log.info("remove key %s in %s", key, sec.path)
        sec.remove_key(key)

    def passphrase_strict(self):
        sec = self.sec()
        key = self.passphrase_key()
        if not sec.exists():
            raise ex.Error("%s does not exist" % sec.path)
        if not sec.has_key(key):
            raise ex.Error("%s has no %s key" % (sec.path, key))
        return sec.decode_key(key)

    def passphrase_new(self):
        sec = self.sec()
        key = self.passphrase_key()
        new_pp = gen_passphrase()
        sec.add_key(key, new_pp)
        return new_pp

    def has_it(self):
        dev = self.get_dev()
        if dev is None:
            return False
        cmd = ["cryptsetup", "isLuks", dev]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        return True

    def is_up(self):
        if not self.has_it():
            return False
        return os.path.exists(self.exposed_dev())

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.label)
            return 0
        self.activate()
        self.can_rollback = True

    def remove_dev_holders(self, devpath, tree):
        dev = tree.get_dev_by_devpath(devpath)
        holders_devpaths = set()
        holder_devs = dev.get_children_bottom_up()
        for holder_dev in holder_devs:
            holders_devpaths |= set(holder_dev.devpath)
        holders_devpaths -= set(dev.devpath)
        holders_handled_by_resources = self.svc.sub_devs() & holders_devpaths
        if len(holders_handled_by_resources) > 0:
            raise ex.Error("resource %s has holders handled by other resources: %s" % (self.rid, ", ".join(holders_handled_by_resources)))
        for holder_dev in holder_devs:
            holder_dev.remove(self)

    def remove_holders(self):
        tree = self.svc.node.devtree
        self.remove_dev_holders(self.exposed_dev(), tree)

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.label)
            return
        self.remove_holders()
        utilities.devices.linux.udevadm_settle()
        self.deactivate()

    def sub_devs(self):
        dev = self.get_dev()
        if dev is None:
            return set([])
        return set([dev])

    def exposed_dev(self):
        name = self.get_name()
        if name is None:
            return
        return "/dev/mapper/%s" % name

    def exposed_devs(self):
        dev = self.exposed_dev()
        if dev is None:
            return set()
        return set([dev])

    def get_dev(self):
        return self.oget("dev")

    def get_name(self):
        if self.name:
            return self.name
        dev = self.get_dev()
        if dev is None:
            return
        return os.path.basename(dev) + "-crypt"

    def verify_passphrase(self):
        if self.svc.options.force:
            return
        try:
            pp = self.passphrase_strict()
        except ex.Error as exc:
            raise ex.Error("abort crypt deactivate, so you can backup the device that we won't be able to activate again: %s. restore the key or use --force to skip this safeguard" % str(exc))

    def deactivate(self):
        if not which('cryptsetup'):
            self.log.debug("cryptsetup command not found")
            return

        dev = self.exposed_dev()
        if dev is None:
            return
        self.verify_passphrase()
        cmd = ["cryptsetup", "luksClose", dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def activate(self):
        if not which('cryptsetup'):
            self.log.debug("cryptsetup command not found")
            return

        dev = self.get_dev()
        if dev is None:
            raise ex.Error("abort luksOpen: no dev")
        name = self.get_name()
        if name is None:
            raise ex.Error("abort luksOpen: no name")
        pp = self.passphrase_strict()
        cmd = ["cryptsetup", "luksOpen", dev, name, "-"]
        self.log.info(" ".join(cmd))
        out, err, ret = justcall(cmd, input=pp)
        if out:
            self.log.info(out)
        if err:
            self.log.error(err)
        if ret != 0:
            raise ex.Error()

    def unprovisioner(self):
        if not which('cryptsetup'):
            self.log.debug("skip crypt unprovision: cryptsetup command not found")
            return

        dev = self.get_dev()
        if dev is not None:
            cmd = ["cryptsetup", "luksErase", "--batch-mode", dev]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.Error
            self.svc.node.unset_lazy("devtree")
        self.forget_passphrase()

    def provisioned(self):
        return self.has_it()

    def provisioner(self):
        if not which('cryptsetup'):
            raise ex.Error("cryptsetup command not found")

        name = self.get_name()
        if name is None:
            raise ex.Error("skip crypt provisioning: no name")

        dev = self.get_dev()
        if dev is None:
            raise ex.Error("skip crypt provisioning: no dev")

        if self.manage_passphrase:
            pp = self.passphrase_new()
        else:
            pp = self.passphrase_strict()

        cmd = [
            "cryptsetup", "luksFormat",
            "--hash", "sha512",
            "--key-size", "512",
            "--cipher", "aes-xts-plain64",
            "--type", "luks2",
            "--batch-mode",
        ]
        if self.fmtlabel:
            cmd += ["--label", self.fmtlabel]
        cmd += [
            dev,
            "-"
        ]
        self.log.info(" ".join(cmd))
        out, err, ret = justcall(cmd, input=pp)
        if out:
            self.log.info(out)
        if err:
            self.log.error(err)
        if ret != 0:
            raise ex.Error()

        self.svc.node.unset_lazy("devtree")

