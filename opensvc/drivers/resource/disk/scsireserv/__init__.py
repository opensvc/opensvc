import os
import time

import core.exceptions as ex
import core.status
from core.resource import Resource


class BaseDiskScsireserv(Resource):
    """Define method to acquire and release scsi SPC-3 persistent reservations
    on devs held by a service
    """

    def __init__(self,
                 rid=None,
                 peer_resource=None,
                 no_preempt_abort=False,
                 prkey=None,
                 **kwargs):
        self.no_preempt_abort = no_preempt_abort
        self.devs = {}
        self.preempt_timeout = 10
        self.prtype = '5'
        self.hostid = None
        self.peer_resource = peer_resource
        self.prkey = prkey
        super(BaseDiskScsireserv, self).__init__(rid=rid+"pr", type="disk.scsireserv", **kwargs)
        self.sort_key = rid


    def mangle_devs(self, devs):
        """
        Can be overidden by child class to apply a mangling the peer
        resource devices
        """
        return dict((dev, [dev]) for dev in devs)


    def set_label(self):
        self.get_devs()
        if len(self.devs) == 0:
            self.label = 'preserv 0 scsi disk'
        elif len(', '.join(sorted(self.devs))) > 248:
            self.label = 'preserv '+', '.join(sorted(self.devs))[0:248]
            self.label += " ..."
        else:
            self.label = ', '.join(sorted(self.devs))


    def get_hostid(self):
        if self.hostid:
            return
        if self.prkey:
            self.hostid = self.prkey
            return
        try:
            self.hostid = self.svc.node.get_prkey()
        except Exception as e:
            raise ex.Error(str(e))


    def _info(self):
        self.get_hostid()
        data = [
            ["prkey", self.hostid],
        ]
        return data


    def scsireserv_supported(self):
        return False


    def ack_unit_attention(self, d):
        raise ex.MissImpl


    def disk_registered(self, disk):
        raise ex.MissImpl


    def disk_register(self, disk):
        raise ex.MissImpl


    def disk_unregister(self, disk):
        raise ex.MissImpl


    def get_reservation_key(self, disk):
        raise ex.MissImpl
        return


    def disk_reserved(self, disk):
        raise ex.MissImpl


    def disk_release(self, disk):
        raise ex.MissImpl


    def disk_reserve(self, disk):
        raise ex.MissImpl


    def _disk_preempt_reservation(self, disk, oldkey):
        raise ex.MissImpl


    def disk_preempt_reservation(self, disk, oldkey):
        if not self.svc.options.force and os.environ.get("OSVC_ACTION_ORIGIN") != "daemon":
            self.log.error("%s is already reserved. use --force to override this safety net"%disk)
            raise ex.Error
        return self._disk_preempt_reservation(disk, oldkey)


    def get_devs(self):
        if len(self.devs) > 0:
            return
        peer_sub_devs = self.peer_resource.sub_devs()
        self.devs = self.mangle_devs(peer_sub_devs)


    def ack_all_unit_attention(self):
        self.get_devs()
        for d in self.devs:
            try:
                if self.ack_unit_attention(d) != 0:
                    return 1
            except ex.ScsiPrNotsupported as exc:
                self.status_log(str(exc))
                continue
        return 0


    def register(self):
        self.log.debug("starting register. prkey %s"%self.hostid)
        self.get_devs()
        self.ack_all_unit_attention()
        r = 0
        for d in self.devs:
            try:
                r += self.disk_register(d)
            except ex.ScsiPrNotsupported as exc:
                self.log.warning(str(exc))
                continue
        return r


    def unregister(self):
        self.log.debug("starting unregister. prkey %s"%self.hostid)
        self.get_devs()
        self.ack_all_unit_attention()
        r = 0
        for d in self.devs:
            try:
                if not self.disk_registered(d):
                    continue
                r += self.disk_unregister(d)
            except ex.ScsiPrNotsupported as exc:
                self.log.warning(str(exc))
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
        self.get_devs()
        self.ack_all_unit_attention()
        r = 0
        for d in self.devs:
            try:
                key = self.get_reservation_key(d) # pylint: disable=assignment-from-none
                if key is None:
                    r += self.disk_reserve(d)
                elif key == self.hostid:
                    continue
                else:
                    r += self.disk_preempt_reservation(d, key)
                    r += self.disk_wait_reservation(d)
            except ex.ScsiPrNotsupported as exc:
                self.log.warning(str(exc))
                continue
        return r


    def release(self):
        self.log.debug("starting release. prkey %s"%self.hostid)
        self.get_devs()
        self.ack_all_unit_attention()
        r = 0
        for d in self.devs:
            try:
                if not self.disk_reserved(d):
                    continue
                r += self.disk_release(d)
            except ex.ScsiPrNotsupported as exc:
                self.log.warning(str(exc))
                continue
        return r


    def clear(self):
        self.log.debug("starting clear. prkey %s"%self.hostid)
        self.get_devs()
        self.ack_all_unit_attention()
        r = 0
        for d in self.devs:
            try:
                if not self.disk_reserved(d):
                    continue
                r += getattr(self, "disk_clear_reservation")(d)
            except ex.ScsiPrNotsupported as exc:
                self.log.warning(str(exc))
                continue
        return r


    def checkreserv(self):
        self.log.debug("starting checkreserv. prkey %s"%self.hostid)
        if self.ack_all_unit_attention() != 0:
            return core.status.WARN
        r = core.status.Status("n/a")
        for d in self.devs:
            try:
                key = self.get_reservation_key(d) # pylint: disable=assignment-from-none
                if key is None:
                    self.log.debug("disk %s is not reserved" % d)
                    r += core.status.DOWN
                elif key != self.hostid:
                    self.log.debug("disk %s is reserved by another host whose key is %s" % (d, key))
                    r += core.status.DOWN
                else:
                    self.log.debug("disk %s is correctly reserved" % d)
                    r += core.status.UP
            except ex.ScsiPrNotsupported as exc:
                self.status_log("%s: pr not supported" % d)
            except ex.Error as exc:
                self.status_log(str(exc))
        return r.status


    def scsireserv(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return 0
        r = 0
        r += self.register()
        r += self.reserve()
        return r


    def scsirelease(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return
        r = 0
        if hasattr(self, "disk_clear_reservation"):
            r += self.clear()
        else:
            r += self.release()
            r += self.unregister()
        return r


    def check_all_paths_registered(self):
        pass


    def _status(self, verbose=False):
        self.set_label()
        try:
            self.get_hostid()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN
        if not self.scsireserv_supported():
            self.status_log("scsi reservation is not supported")
            return core.status.NA
        try:
            self.check_all_paths_registered()
        except ex.Signal as exc:
            self.status_log(str(exc))
        except ex.Error as exc:
            self.status_log(str(exc))
            return core.status.WARN
        return self.checkreserv()


    def start(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return
        if self._status() == core.status.UP:
            self.log.info("already started")
            return
        self.can_rollback = True
        if self.scsireserv() != 0:
            raise ex.Error

    def post_provision_start(self):
        def start_and_check():
            self.start()
            time.sleep(5)
            if self._status() in (core.status.UP, core.status.NA):
                self.log.info("registration and reservation stability check passed")
                return True
            return False
        retries = 5
        self.wait_for_fn(
            lambda: start_and_check(),
            retries, 1,
            errmsg="not registered after %d registrations attempts" % retries
        )

    def stop(self):
        self.get_hostid()
        if not self.scsireserv_supported():
            return
        if self.scsirelease() != 0:
            raise ex.Error


    def boot(self):
        self.stop()


    def is_provisioned(self, refresh=False):
        return True
