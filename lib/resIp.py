from __future__ import unicode_literals

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
        self.ipDev = ipDev
        self.ipName = ipName
        self.mask = mask
        self.gateway = gateway
        self.set_label()

    def set_label(self):
        self.label = str(self.ipName) + '@' + self.ipDev

    def info(self):
        if self.ipName is None:
            return
        try:
            self.getaddr()
        except ex.excError:
            pass
        from rcUtilities import to_cidr
        data = [
          ["ipaddr", self.addr],
          ["ipname", self.ipName],
          ["ipdev", self.ipDev],
          ["mask", str(to_cidr(self.mask))],
          ["gateway", str(self.gateway)],
        ]
        return self.fmt_info(data)

    def getaddr(self, cache_fallback=False):
        if self.ipName is None:
            raise ex.excError("ip address is not allocated yet")
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
            return rcStatus.UNDEF
        ifconfig = rcIfconfig.ifconfig()
        intf = ifconfig.interface(self.ipDev)
        if intf is None and not "dedicated" in self.tags:
            self.status_log("interface %s not found" % self.ipDev)
            return rcStatus.DOWN
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
            return rcStatus.UNDEF

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
        if not self.svc.abort_start_done and self.check_ping():
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
        if self.ipName is None:
            self.log.warning("skip start: no ipname set")
            return
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

        try:
            self.dns_update()
        except ex.excError as exc:
            self.log.error(str(exc))

    def dns_update(self):
        """
        Post a dns update request to the collector.
        """
        from svcBuilder import conf_get_string_scope, conf_get_boolean_scope

        if self.ipName is None:
            self.log.debug("skip dns update: ipname is not set")
            return

        try:
            dns_update = conf_get_boolean_scope(self.svc, self.svc.config, self.rid, "dns_update")
        except ex.OptNotFound:
            self.log.debug("skip dns update: dns_update is not set")
            return

        if not self.is_up():
            self.log.debug("skip dns update: resource is not up")
            return

        try:
            dns_name_suffix = conf_get_string_scope(self.svc, self.svc.config, self.rid, "dns_name_suffix")
        except ex.OptNotFound:
            dns_name_suffix = None
            self.log.debug("dns update: dns_name_suffix is not set")

        try:
            self.getaddr()
        except ex.excError as exc:
            self.log.error(str(exc))
            return

        post_data = {
            "content": self.addr,
        }

        if dns_name_suffix:
            post_data["name"] = dns_name_suffix

        try:
            data = self.svc.node.collector_rest_post(
                "/dns/services/records",
                post_data,
                svcname=self.svc.svcname,
            )
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(data["error"])

        self.log.info("dns updated")


    def stop(self):
        if self.ipName is None:
            self.log.warning("skip stop: no ipname set")
            return
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

    def allocate(self):
        """
        Request an ip in the ipdev network from the collector.
        """
        from svcBuilder import conf_get_string_scope
        import ipaddress

        try:
            ipname = conf_get_string_scope(self.svc, self.svc.config, self.rid, "ipname")
            self.log.info("skip allocate: an ip is already defined")
            return
        except ex.OptNotFound:
            pass

        try:
            # explicit network setting
            network = conf_get_string_scope(self.svc, self.svc.config, self.rid, "network")
        except ex.OptNotFound:
            network = None

        if network is None:
            # implicit network: the network of the first ipdev ip
            ifconfig = rcIfconfig.ifconfig()
            intf = ifconfig.interface(self.ipDev)
            if isinstance(intf.ipaddr, list):
                baseaddr = intf.ipaddr[0]
            else:
                baseaddr = intf.ipaddr
            network = str(ipaddress.IPv4Interface(baseaddr).network.network_address)

        post_data = {
            "network": network,
        }

        try:
            post_data["name"] = conf_get_string_scope(self.svc, self.svc.config, self.rid, "dns_name_suffix")
        except ex.OptNotFound:
            self.log.debug("allocate: dns_name_suffix is not set")

        try:
            data = self.svc.node.collector_rest_post(
                "/networks/%s/allocate" % network,
                post_data,
                svcname=self.svc.svcname,
            )
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(data["error"])

        if "info" in data:
            self.log.info(data["info"])

        self.ipName = data["data"]["ip"]
        self.addr = self.ipName
        self.set_label()
        self.svc._set(self.rid, "ipname", self.ipName)
        self.log.info("ip %s allocated" % self.ipName)

    def release(self):
        """
        Release an allocated ip a collector managed network.
        """
        from svcBuilder import conf_get_string_scope

        if self.ipName is None:
            self.log.info("skip release: no ipname set")
            return

        try:
            self.getaddr()
        except ex.excError:
            self.log.info("skip release: ipname does not resolve to an address")
            return

        post_data = {}

        try:
            post_data["name"] = conf_get_string_scope(self.svc, self.svc.config, self.rid, "dns_name_suffix")
        except ex.OptNotFound:
            self.log.debug("allocate: dns_name_suffix is not set")

        try:
            data = self.svc.node.collector_rest_post(
                "/networks/%s/release" % self.addr,
                post_data,
                svcname=self.svc.svcname,
            )
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(data["error"])

        if "info" in data:
            self.log.info(data["info"])

        self.svc._unset(self.rid, "ipname")
        self.log.info("ip %s released" % self.ipName)


    def provision(self):
        self.allocate()
        self.start()

    def unprovision(self):
        self.stop()
        self.release()


if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

    print("""i1=Ip("eth0","192.168.0.173")""")
    i=Ip("eth0","192.168.0.173")
    print("show i", i)
    print("""i.do_action("start")""")
    i.do_action("start")

