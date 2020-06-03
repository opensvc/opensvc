"""
Base disk resource driver module.
"""

import os

import core.status
from core.resource import Resource

KW_PRKEY = {
    "keyword": "prkey",
    "at": True,
    "text": "Defines a specific persistent reservation key for the resource. Takes priority over the service-level defined prkey and the node.conf specified prkey."
}
KW_PROMOTE_RW = {
    "keyword": "promote_rw",
    "default": False,
    "convert": "boolean",
    "candidates": (True, False),
    "text": "If set to ``true``, OpenSVC will try to promote the base devices to read-write on start."
}
KW_NO_PREEMPT_ABORT = {
    "keyword": "no_preempt_abort",
    "at": True,
    "candidates": (True, False),
    "default": False,
    "convert": "boolean",
    "text": "If set to ``true``, OpenSVC will preempt scsi reservation with a preempt command instead of a preempt and and abort. Some scsi target implementations do not support this last mode (esx). If set to ``false`` or not set, :kw:`no_preempt_abort` can be activated on a per-resource basis."
}
KW_SCSIRESERV = {
    "keyword": "scsireserv",
    "default": False,
    "convert": "boolean",
    "candidates": (True, False),
    "text": "If set to ``true``, OpenSVC will try to acquire a type-5 (write exclusive, registrant only) scsi3 persistent reservation on every path to every disks held by this resource. Existing reservations are preempted to not block service start-up. If the start-up was not legitimate the data are still protected from being written over from both nodes. If set to ``false`` or not set, :kw:`scsireserv` can be activated on a per-resource basis."
}

BASE_KEYWORDS = [
    KW_PRKEY,
    KW_PROMOTE_RW,
    KW_NO_PREEMPT_ABORT,
    KW_SCSIRESERV,
]

class BaseDisk(Resource):
    """
    Base disk resource driver, derived for LVM, Veritas, ZFS, ...
    """

    def __init__(self, name=None, **kwargs):
        super(BaseDisk, self).__init__(**kwargs)
        self.name = name

    def __str__(self):
        return "%s name=%s" % (super(BaseDisk, self).__str__(), self.name)

    def has_it(self): return False
    def is_up(self): return False
    def do_start(self): return False
    def do_stop(self): return False

    def stop(self):
        self.do_stop()

    def start(self):
        self.promote_rw()
        self.do_start()

    def _status(self, verbose=False):
        if self.is_up():
            return core.status.UP
        else:
            return core.status.DOWN

    def create_static_name(self, dev, suffix="0"):
        d = self.create_dev_dir()
        lname = self.rid.replace("#", ".") + "." + suffix
        l = os.path.join(d, lname)
        if os.path.exists(l) and os.path.realpath(l) == dev:
            return
        self.log.info("create static device name %s -> %s" % (l, dev))
        try:
            os.unlink(l)
        except:
            pass
        os.symlink(dev, l)

    def create_dev_dir(self):
        d = os.path.join(self.svc.var_d, "dev")
        if not os.path.exists(d):
            os.makedirs(d, 0o755)
        return d
