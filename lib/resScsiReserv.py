import resources as Res
import uuid
import re
import time
import rcStatus
import rcExceptions as ex
from rcUtilities import which
from subprocess import *
from rcGlobalEnv import rcEnv
hostId = __import__('hostid'+rcEnv.sysname)


class ScsiReserv(Res.Resource):
    """Define method to acquire and release scsi SPC-3 persistent reservations
    on disks held by a service
    """
    def __init__(self,
                 rid=None,
                 peer_resource=None,
                 no_preempt_abort=False,
                 prkey=None,
                 **kwargs):
        self.no_preempt_abort = no_preempt_abort
        self.disks = set([])
        self.preempt_timeout = 10
        self.prtype = '5'
        self.hostid = None
        self.peer_resource = peer_resource
        self.prkey = prkey
        Res.Resource.__init__(self,
                              rid=rid+"pr",
                              type="disk.scsireserv",
                              **kwargs)

    def set_label(self):
        self.get_disks()
        if len(self.disks) == 0:
            self.label = 'preserv 0 scsi disk'
        elif len(', '.join(self.disks)) > 248:
            self.label = 'preserv '+', '.join(self.disks)[0:248]
            self.label += " ..."
        else:
            self.label = ', '.join(self.disks)

    def get_hostid(self):
        if self.hostid:
            return
        if self.prkey:
            self.hostid = self.prkey
            return
        try:
            self.hostid = self.svc.node.get_prkey()
        except Exception as e:
            raise ex.excError(str(e))

    def info(self):
        self.get_hostid()
        data = [
          [self.svc.svcname, self.svc.node.nodename, self.svc.clustertype, self.rid, "prkey", self.hostid],
        ]
        return data

    def scsireserv_supported(self):
        return False

    def ack_unit_attention(self, d):
        raise ex.notImplemented

    def disk_registered(self, disk):
        raise ex.notImplemented

    def disk_register(self, disk):
        raise ex.notImplemented

    def disk_unregister(self, disk):
        raise ex.notImplemented

    def get_reservation_key(self, disk):
        raise ex.notImplemented

    def disk_reserved(self, disk):
        raise ex.notImplemented

    def disk_release(self, disk):
        raise ex.notImplemented

    def disk_reserve(self, disk):
        raise ex.notImplemented

    def disk_preempt_reservation(self, disk, oldkey):
        if not hasattr(self, '_disk_preempt_reservation'):
            raise ex.notImplemented
        if not self.svc.options.force and not self.svc.options.cluster:
            self.log.error("%s is already reserved. use --force to override this safety net"%disk)
            raise ex.excError
        return self._disk_preempt_reservation(disk, oldkey)

    def get_disks(self):
        if len(self.disks) > 0:
            return
        self.disks = self.peer_resource.disklist()

    def ack_all_unit_attention(self):
        self.get_disks()
        for d in self.disks:
            try:
                if self.ack_unit_attention(d) != 0:
                    return 1
            except ex.excScsiPrNotsupported:
                continue
        return 0

    def register(self):
        self.log.debug("starting register. prkey %s"%self.hostid)
        self.get_disks()
        r = 0
        for d in self.disks:
            try:
                r += self.ack_unit_attention(d)
                r += self.disk_register(d)
            except ex.excScsiPrNotsupported:
                continue
        return r

    def unregister(self):
        self.log.debug("starting unregister. prkey %s"%self.hostid)
        self.get_disks()
        r = 0
        for d in self.disks:
            try:
                r += self.ack_unit_attention(d)
                if not self.disk_registered(d):
                    continue
                r += self.disk_unregister(d)
            except ex.excScsiPrNotsupported:
                continue
        return r

    def disk_wait_reservation(self, disk):
        for i in range(3, 0, -1):
            if self.disk_reserved(disk):
                self.log.info("reservation acquired for disk %s" % disk)
                return 0
            if i > 0:
                time.sleep(1)
        self.log.error("timed out waiting for reservation for disk %s" % disk)
        return 1

    def reserve(self):
        self.log.debug("starting reserve. prkey %s"%self.hostid)
        self.get_disks()
        r = 0
        for d in self.disks:
            try:
                r += self.ack_unit_attention(d)
                key = self.get_reservation_key(d)
                if key is None:
                    r += self.disk_reserve(d)
                elif key == self.hostid:
                    continue
                else:
                    r += self.disk_preempt_reservation(d, key)
                    r += self.disk_wait_reservation(d)
            except ex.excScsiPrNotsupported:
                continue
        return r

    def release(self):
        self.log.debug("starting release. prkey %s"%self.hostid)
        self.get_disks()
        r = 0
        for d in self.disks:
            try:
                r += self.ack_unit_attention(d)
                if not self.disk_reserved(d):
                    continue
                r += self.disk_release(d)
            except ex.excScsiPrNotsupported:
                continue
        return r

    def clear(self):
        self.log.debug("starting clear. prkey %s"%self.hostid)
        self.get_disks()
        r = 0
        for d in self.disks:
            try:
                r += self.ack_unit_attention(d)
                if not self.disk_reserved(d):
                    continue
                r += self.disk_clear_reservation(d)
            except ex.excScsiPrNotsupported:
                continue
        return r

    def checkreserv(self):
        self.log.debug("starting checkreserv. prkey %s"%self.hostid)
        if self.ack_all_unit_attention() != 0:
            return rcStatus.WARN
        r = rcStatus.Status()
        for d in self.disks:
            try:
                key = self.get_reservation_key(d)
                if key is None:
                    self.log.debug("disk %s is not reserved" % d)
                    r += rcStatus.DOWN
                elif key != self.hostid:
                    self.log.debug("disk %s is reserved by another host whose key is %s" % (d, key))
                    r += rcStatus.DOWN
                else:
                    self.log.debug("disk %s is correctly reserved" % d)
                    r += rcStatus.UP
            except ex.excScsiPrNotsupported:
                continue
        return r.status

    def scsireserv(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return
        r = 0
        r += self.register()
        r += self.reserve()
        return r

    def scsirelease(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return
        r = 0
        if hasattr(self, 'disk_clear_reservation'):
            r += self.clear()
        else:
            r += self.release()
            r += self.unregister()
        return r

    def scsicheckreserv(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return
        return self.checkreserv()

    def _status(self, verbose=False):
        self.set_label()
        try:
            self.get_hostid()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN
        if not self.scsireserv_supported():
            return rcStatus.NA
        return self.checkreserv()

    def start(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return
        if self._status() == rcStatus.UP:
            self.log.info("already started")
            return
        self.can_rollback = True
        if self.scsireserv() != 0:
            raise ex.excError

    def stop(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return
        if self.scsirelease() != 0:
            raise ex.excError

    def provision(self):
        self.start()

    def unprovision(self):
        self.stop()

