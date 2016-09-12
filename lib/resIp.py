import resources as Res
from rcGlobalEnv import *

from rcUtilities import qcall, which, getaddr
rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)
import rcStatus
import rcExceptions as ex
import os

class Ip(Res.Resource):
    """ basic ip resource
    """
    def __init__(self,
                 rid=None,
                 ipDev=None,
                 ipName=None,
                 mask=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0,
                 subset=None,
                 gateway=None):
        Res.Resource.__init__(self,
                              rid,
                              "ip",
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset,
                              monitor=monitor,
                              always_on=always_on,
                              restart=restart)
        self.ipDev=ipDev
        self.ipName=ipName
        self.mask=mask
        self.label = ipName + '@' + ipDev
        self.gateway = gateway

    def info(self):
        data = [
          [self.rid, "ipname", self.ipName],
          [self.rid, "ipdev", self.ipDev],
          [self.rid, "mask", str(self.mask)],
          [self.rid, "gateway", str(self.gateway)],
        ]
        return self.fmt_info(data)

    def getaddr(self, cache_fallback=False):
        if hasattr(self, 'addr'):
            return
        try:
            self.log.debug("resolving %s" % self.ipName)
            self.addr = getaddr(self.ipName, cache_fallback=cache_fallback, log=self.log)
        except Exception as e:
            if not self.disabled:
                raise ex.excError("could not resolve name %s: %s" % (self.ipName, str(e)))

    def __str__(self):
        return "%s ipdev=%s ipname=%s" % (Res.Resource.__str__(self),\
                                         self.ipDev, self.ipName)
    def setup_environ(self):
        os.environ['OPENSVC_IPDEV'] = str(self.ipDev)
        os.environ['OPENSVC_IPNAME'] = str(self.ipName)
        os.environ['OPENSVC_MASK'] = str(self.mask)
        try:
            self.getaddr()
            os.environ['OPENSVC_IPADDR'] = str(self.addr)
        except:
            pass
        l = self.rid.split('#')
        if len(l) == 2:
            index = l[1]
        else:
            index = ''
        var = 'OPENSVC_IP'+index
        l = []
        for p in ['ipName', 'ipDev', 'addr', 'mask']:
            if hasattr(self, p):
                l.append(str(getattr(self, p)))
            else:
                l.append('unknown')
        val = ' '.join(l)
        os.environ[var] = val

    def _status(self, verbose=False):
        try:
            self.getaddr()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN
        ifconfig = rcIfconfig.ifconfig()
        intf = ifconfig.interface(self.ipDev)
        if intf is None and not "dedicated" in self.tags:
            self.status_log("interface %s not found" % self.ipDev)
            return rcStatus.WARN
        try:
            if self.is_up():
                return self.status_stdby(rcStatus.UP)
            else:
                return self.status_stdby(rcStatus.DOWN)
        except ex.excNotSupported:
            self.status_log("not supported")
            return rcStatus.UNDEF
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN

    def arp_announce(self):
        if ':' in self.addr:
            return
        if not which("arping"):
            self.log.warning("arp announce skipped. install 'arping'")
            return
        cmd = ["arping", "-U", "-c", "1", "-I", self.ipDev, "-s", self.addr, self.addr]
        self.log.info(' '.join(cmd))
        qcall(cmd)

    def abort_start(self):
        self.abort_start_done = True
        if 'nonrouted' in self.tags or 'noaction' in self.tags:
            return False
        if not hasattr(self, "addr"):
            return False
        if not self.is_up() and self.check_ping():
            return True
        return False

    def check_ping(self, count=1, timeout=5):
        raise ex.MissImpl('check_ping')

    def startip_cmd(self):
        raise ex.MissImpl('startip_cmd')

    def stopip_cmd(self):
        raise ex.MissImpl('stopip_cmd')

    def is_up(self):
        ifconfig = self.get_ifconfig()
        if ifconfig.has_param("ipaddr", self.addr) is not None or \
           ifconfig.has_param("ip6addr", self.addr) is not None:
            self.log.debug("%s@%s is up" % (self.addr, self.ipDev))
            return True
        self.log.debug("%s@%s is down" % (self.addr, self.ipDev))
        return False

    def allow_start(self):
        if self.is_up() is True:
            self.log.info("%s is already up on %s" % (self.addr, self.ipDev))
            raise ex.IpAlreadyUp(self.addr)
        ifconfig = rcIfconfig.ifconfig()
        intf = ifconfig.interface(self.ipDev)
        if intf is None:
            self.log.error("interface %s not found. Cannot stack over it." % self.ipDev)
            raise ex.IpDevDown(self.ipDev)
        if not intf.flag_up:
            if hasattr(intf, 'groupname') and intf.groupname != "":
                l = [ i for i in ifconfig.get_matching_interfaces('groupname', intf.groupname) if i.flag_up]
                if len(l) == 1:
                    self.log.info("switch %s to valid alternate path %s" % (self.ipDev, l[0].name))
                    intf = l[0]
                    self.ipDev = l[0].name
            elif hasattr(self, "start_link"):
                    self.start_link()
            else:
                self.log.error("interface %s is not up. Cannot stack over it." % self.ipDev)
                raise ex.IpDevDown(self.ipDev)
        if not hasattr(self, 'abort_start_done') and self.check_ping():
            self.log.error("%s is already up on another host" % (self.addr))
            raise ex.IpConflict(self.addr)
        return

    def lock(self, timeout=60, delay=1):
        import lock
        lockfile = os.path.join(rcEnv.pathlock, 'startip')
        lockfd = None
        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except lock.lockTimeout:
            self.log.error("timed out waiting for lock")
            raise ex.excError
        except lock.lockNoLockFile:
            self.log.error("lock_nowait: set the 'lockfile' param")
            raise ex.excError
        except lock.lockCreateError:
            self.log.error("can not create lock file %s"%lockfile)
            raise ex.excError
        except lock.lockAcquire as e:
            self.log.warn("another action is currently running (pid=%s)"%e.pid)
            raise ex.excError
        except ex.excSignal:
            self.log.error("interrupted by signal")
            raise ex.excError
        except:
            self.save_exc()
            raise ex.excError("unexpected locking error")
        self.lockfd = lockfd

    def unlock(self):
        import lock
        lock.unlock(self.lockfd)

    def get_ifconfig(self):
        return rcIfconfig.ifconfig()

    def start(self):
        self.getaddr()
        try:
            self.allow_start()
        except (ex.IpConflict, ex.IpDevDown):
            raise ex.excError
        except (ex.IpAlreadyUp, ex.IpNoActions):
            return
        self.log.debug('pre-checks passed')

        self.lock()
        ifconfig = self.get_ifconfig()
        if self.mask is None:
            intf = ifconfig.interface(self.ipDev)
            if intf is None:
                self.log.error("netmask parameter is mandatory with 'noalias' tag")
                self.unlock()
                raise ex.excError
            self.mask = intf.mask
        if self.mask == '':
            self.log.error("No netmask set on parent interface %s" % self.ipDev)
            self.unlock()
            raise ex.excError
        elif isinstance(self.mask, list):
            if len(self.mask) > 0:
                self.mask = self.mask[0]
            else:
                self.log.error("No netmask set on parent interface %s" % self.ipDev)
                self.unlock()
                raise ex.excError
        if 'noalias' in self.tags:
            self.stacked_dev = self.ipDev
        else:
            self.stacked_dev = ifconfig.get_stacked_dev(self.ipDev,\
                                                        self.addr,\
                                                        self.log)
        if self.stacked_dev is None:
            self.log.error("could not determine a stacked dev for parent interface %s" % self.ipDev)
            self.unlock()
            raise ex.excError

        arp_announce = True
        try:
            (ret, out, err) = self.startip_cmd()
            self.can_rollback = True
        except ex.excNotSupported:
            self.log.info("start ip not supported")
            ret = 0
            out = ""
            err = ""
            arp_announce = False
            pass

        self.unlock()
        if ret != 0:
            self.log.error("failed")
            raise ex.excError

        if arp_announce:
            self.arp_announce()

    def stop(self):
        self.getaddr(cache_fallback=True)
        if self.is_up() is False:
            self.log.info("%s is already down on %s" % (self.addr, self.ipDev))
            return
        ifconfig = self.get_ifconfig()
        if 'noalias' in self.tags:
            self.stacked_dev = self.ipDev
        else:
            self.stacked_dev = ifconfig.get_stacked_dev(self.ipDev,\
                                                        self.addr,\
                                                        self.log)
        if self.stacked_dev is None:
            raise ex.excError

        try:
            (ret, out, err) = self.stopip_cmd()
        except ex.excNotSupported:
            self.log.info("stop ip not supported")
            return

        if ret != 0:
            self.log.error("failed")
            raise ex.excError

        import time
        tmo = 15
        for i in range(tmo):
            if not self.check_ping(count=1, timeout=1):
                break
            time.sleep(1)

        if i == tmo-1:
            self.log.error("%s refuse to go down"%self.addr)
            raise ex.excError

    def provision(self):
        m = __import__("provIp")
        prov = m.ProvisioningIp(self)
        prov.provisioner()


if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

    print("""i1=Ip("eth0","192.168.0.173")""")
    i=Ip("eth0","192.168.0.173")
    print("show i", i)
    print("""i.do_action("start")""")
    i.do_action("start")

