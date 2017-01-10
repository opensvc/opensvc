"""
Generic ip resource driver.
"""
from __future__ import unicode_literals

import os

import resources as Res
from rcGlobalEnv import rcEnv
from rcUtilities import qcall, which, getaddr
import rcStatus
import rcExceptions as ex

IFCONFIG_MOD = __import__('rcIfconfig'+rcEnv.sysname)

class Ip(Res.Resource):
    """ basic ip resource
    """
    def __init__(self,
                 rid=None,
                 ipdev=None,
                 ipname=None,
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
        self.ipdev = ipdev
        self.ipname = ipname
        self.mask = mask
        self.gateway = gateway
        self.set_label()
        self.lockfd = None
        self.stacked_dev = None
        self.addr = None

    def set_label(self):
        """
        Set the resource label property.
        """
        self.label = str(self.ipname) + '@' + self.ipdev

    def info(self):
        """
        Contribute resource key/val pairs to the service's resinfo.
        """
        if self.ipname is None:
            return
        try:
            self.getaddr()
        except ex.excError:
            pass
        from rcUtilities import to_cidr
        data = [
            ["ipaddr", self.addr],
            ["ipname", self.ipname],
            ["ipdev", self.ipdev],
            ["mask", str(to_cidr(self.mask))],
            ["gateway", str(self.gateway)],
        ]
        return self.fmt_info(data)

    def getaddr(self, cache_fallback=False):
        """
        Try resolving the ipname into an ip address. If the resolving fails and
        <cache_fallback> is True, use the last successful resolution result.
        """
        if self.ipname is None:
            raise ex.excError("ip address is not allocated yet")
        if self.addr is not None:
            return
        try:
            self.log.debug("resolving %s", self.ipname)
            self.addr = getaddr(self.ipname, cache_fallback=cache_fallback, log=self.log)
        except Exception as exc:
            if not self.disabled:
                raise ex.excError("could not resolve name %s: %s" % (self.ipname, str(exc)))

    def __str__(self):
        return "%s ipdev=%s ipname=%s" % (Res.Resource.__str__(self),\
                                         self.ipdev, self.ipname)
    def setup_environ(self):
        """
        Set the main resource properties as environment variables, so they
        are available to triggers.
        """
        os.environ['OPENSVC_IPDEV'] = str(self.ipdev)
        os.environ['OPENSVC_IPNAME'] = str(self.ipname)
        os.environ['OPENSVC_MASK'] = str(self.mask)
        try:
            self.getaddr()
            os.environ['OPENSVC_IPADDR'] = str(self.addr)
        except:
            pass
        elements = self.rid.split('#')
        if len(elements) == 2:
            index = elements[1]
        else:
            index = ''
        var = 'OPENSVC_IP'+index
        vals = []
        for prop in ['ipname', 'ipdev', 'addr', 'mask']:
            if getattr(self, prop) is not None:
                vals.append(str(getattr(self, prop)))
            else:
                vals.append('unknown')
        val = ' '.join(vals)
        os.environ[var] = val

    def _status(self, verbose=False):
        """
        Evaluate the ip resource status.
        """
        try:
            self.getaddr()
        except Exception as exc:
            self.status_log(str(exc))
            return rcStatus.UNDEF
        ifconfig = IFCONFIG_MOD.ifconfig()
        intf = ifconfig.interface(self.ipdev)
        if intf is None and "dedicated" not in self.tags:
            self.status_log("interface %s not found" % self.ipdev)
            return rcStatus.DOWN
        try:
            if self.is_up():
                return self.status_stdby(rcStatus.UP)
            else:
                return self.status_stdby(rcStatus.DOWN)
        except ex.excNotSupported:
            self.status_log("not supported")
            return rcStatus.UNDEF
        except ex.excError as exc:
            self.status_log(str(exc))
            return rcStatus.UNDEF

    def arp_announce(self):
        """
        Announce to neighbors the ip address is plumbed on ipdev through a
        arping broadcast of unsollicited packets.
        """
        if ':' in self.addr:
            return
        if not which("arping"):
            self.log.warning("arp announce skipped. install 'arping'")
            return
        cmd = ["arping", "-U", "-c", "1", "-I", self.ipdev, "-s", self.addr, self.addr]
        self.log.info(' '.join(cmd))
        qcall(cmd)

    def abort_start(self):
        """
        Return True if the service start should be aborted because of a routed
        ip conflict.
        """
        if 'nonrouted' in self.tags or 'noaction' in self.tags:
            return False
        if self.addr is None:
            return False
        if not self.is_up() and self.check_ping():
            return True
        return False

    def start_link(self):
        """
        Start the ipdev link.
        """
        raise ex.MissImpl('start_link')

    def check_ping(self, count=1, timeout=5):
        """
        Test if the ip is seen as active on the newtorks.
        """
        raise ex.MissImpl('check_ping')

    def startip_cmd(self):
        """
        The os/driver specific start implementation.
        """
        raise ex.MissImpl('startip_cmd')

    def stopip_cmd(self):
        """
        The os/driver specific stop implementation.
        """
        raise ex.MissImpl('stopip_cmd')

    def is_up(self):
        """
        Return True if the ip is plumbed.
        """
        ifconfig = self.get_ifconfig()
        if ifconfig.has_param("ipaddr", self.addr) is not None or \
           ifconfig.has_param("ip6addr", self.addr) is not None:
            self.log.debug("%s@%s is up", self.addr, self.ipdev)
            return True
        self.log.debug("%s@%s is down", self.addr, self.ipdev)
        return False

    def allow_start(self):
        """
        Do sanity checks before allowing the start.
        """
        if self.is_up() is True:
            self.log.info("%s is already up on %s", self.addr, self.ipdev)
            raise ex.IpAlreadyUp(self.addr)
        ifconfig = IFCONFIG_MOD.ifconfig()
        intf = ifconfig.interface(self.ipdev)
        if intf is None:
            self.log.error("interface %s not found. Cannot stack over it.", self.ipdev)
            raise ex.IpDevDown(self.ipdev)
        if not intf.flag_up:
            if hasattr(intf, 'groupname') and intf.groupname != "":
                l = [_intf for _intf in ifconfig.get_matching_interfaces('groupname', intf.groupname) if _intf.flag_up]
                if len(l) == 1:
                    self.log.info("switch %s to valid alternate path %s", self.ipdev, l[0].name)
                    intf = l[0]
                    self.ipdev = l[0].name
            try:
                self.start_link()
            except ex.MissImpl:
                self.log.error("interface %s is not up. Cannot stack over it.", self.ipdev)
                raise ex.IpDevDown(self.ipdev)
        if not self.svc.abort_start_done and self.check_ping():
            self.log.error("%s is already up on another host", self.addr)
            raise ex.IpConflict(self.addr)
        return

    def lock(self):
        """
        Acquire the startip lock, protecting against allocation of the same
        ipdev stacked device to multiple resources or multiple services.
        """
        import lock
        if self.svc.options.waitlock >= 0:
            timeout = self.svc.options.waitlock
        else:
            timeout = 120
        delay = 1
        lockfd = None
        action = "startip"
        lockfile = os.path.join(rcEnv.pathlock, action)
        details = "(timeout %d, delay %d, action %s, lockfile %s)" % \
                  (timeout, delay, action, lockfile)
        self.log.debug("acquire startip lock %s", details)

        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile, intent="startip")
        except lock.lockTimeout as exc:
            raise ex.excError("timed out waiting for lock %s: %s" % (details, str(exc)))
        except lock.lockNoLockFile:
            raise ex.excError("lock_nowait: set the 'lockfile' param %s" % details)
        except lock.lockCreateError:
            raise ex.excError("can not create lock file %s" % details)
        except lock.lockAcquire as exc:
            raise ex.excError("another action is currently running %s: %s" % (details, str(exc)))
        except ex.excSignal:
            raise ex.excError("interrupted by signal %s" % details)
        except Exception as exc:
            self.save_exc()
            raise ex.excError("unexpected locking error %s: %s" % (details, str(exc)))

        if lockfd is not None:
            self.lockfd = lockfd

    def unlock(self):
        """
        Release the startip lock.
        """
        import lock
        lock.unlock(self.lockfd)

    @staticmethod
    def get_ifconfig():
        """
        Wrapper around the os specific rcIfconfig module's ifconfig function.
        Return a parsed ifconfig dataset.
        """
        return IFCONFIG_MOD.ifconfig()

    def start(self):
        """
        Start the resource.
        """
        if self.ipname is None:
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
        try:
            arp_announce = self.start_locked()
        finally:
            self.unlock()

        if arp_announce:
            self.arp_announce()

        try:
            self.dns_update()
        except ex.excError as exc:
            self.log.error(str(exc))

    def start_locked(self):
        """
        The start codepath fragment protected by the startip lock.
        """
        ifconfig = self.get_ifconfig()
        if self.mask is None:
            intf = ifconfig.interface(self.ipdev)
            if intf is None:
                raise ex.excError("netmask parameter is mandatory with 'noalias' tag")
            self.mask = intf.mask
        if self.mask == '':
            raise ex.excError("No netmask set on parent interface %s" % self.ipdev)
        elif isinstance(self.mask, list):
            if len(self.mask) > 0:
                self.mask = self.mask[0]
            else:
                raise ex.excError("No netmask set on parent interface %s" % self.ipdev)
        if 'noalias' in self.tags:
            self.stacked_dev = self.ipdev
        else:
            self.stacked_dev = ifconfig.get_stacked_dev(self.ipdev,\
                                                        self.addr,\
                                                        self.log)
        if self.stacked_dev is None:
            raise ex.excError("could not determine a stacked dev for parent "
                              "interface %s" % self.ipdev)

        arp_announce = True
        try:
            ret = self.startip_cmd()[0]
            self.can_rollback = True
        except ex.excNotSupported:
            self.log.info("start ip not supported")
            ret = 0
            arp_announce = False

        if ret != 0:
            raise ex.excError("failed")

        return arp_announce

    def dns_update(self):
        """
        Post a dns update request to the collector.
        """
        from svcBuilder import conf_get_string_scope, conf_get_boolean_scope

        if self.ipname is None:
            self.log.debug("skip dns update: ipname is not set")
            return

        try:
            conf_get_boolean_scope(self.svc, self.svc.config, self.rid,
                                   "dns_update")
        except ex.OptNotFound:
            self.log.debug("skip dns update: dns_update is not set")
            return

        if not self.is_up():
            self.log.debug("skip dns update: resource is not up")
            return

        try:
            dns_name_suffix = conf_get_string_scope(self.svc, self.svc.config,
                                                    self.rid, "dns_name_suffix")
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
        """
        Stop the resource.
        """
        if self.ipname is None:
            self.log.warning("skip stop: no ipname set")
            return
        self.getaddr(cache_fallback=True)
        if self.is_up() is False:
            self.log.info("%s is already down on %s", self.addr, self.ipdev)
            return
        ifconfig = self.get_ifconfig()
        if 'noalias' in self.tags:
            self.stacked_dev = self.ipdev
        else:
            self.stacked_dev = ifconfig.get_stacked_dev(self.ipdev,\
                                                        self.addr,\
                                                        self.log)
        if self.stacked_dev is None:
            raise ex.excError

        try:
            ret = self.stopip_cmd()[0]
        except ex.excNotSupported:
            self.log.info("stop ip not supported")
            return

        if ret != 0:
            self.log.error("failed")
            raise ex.excError

        import time
        tmo = 15
        idx = 0
        for idx in range(tmo):
            if not self.check_ping(count=1, timeout=1):
                break
            time.sleep(1)

        if idx == tmo-1:
            self.log.error("%s refuse to go down", self.addr)
            raise ex.excError

    def allocate(self):
        """
        Request an ip in the ipdev network from the collector.
        """
        from svcBuilder import conf_get_string_scope
        import ipaddress

        try:
            conf_get_string_scope(self.svc, self.svc.config, self.rid, "ipname")
            self.log.info("skip allocate: an ip is already defined")
            return
        except ex.OptNotFound:
            pass

        if self.ipdev is None:
            self.log.info("skip allocate: ipdev is not set")
            return

        try:
            # explicit network setting
            network = conf_get_string_scope(self.svc, self.svc.config, self.rid, "network")
        except ex.OptNotFound:
            network = None

        if network is None:
            # implicit network: the network of the first ipdev ip
            ifconfig = IFCONFIG_MOD.ifconfig()
            intf = ifconfig.interface(self.ipdev)
            try:
                if isinstance(intf.ipaddr, list):
                    baseaddr = intf.ipaddr[0]
                else:
                    baseaddr = intf.ipaddr
                network = str(ipaddress.IPv4Interface(baseaddr).network.network_address)
            except ValueError:
                self.log.info("skip allocate: ipdev has no configured address "
                              "and network is not set")
                return

        post_data = {
            "network": network,
        }

        try:
            post_data["name"] = conf_get_string_scope(self.svc, self.svc.config,
                                                      self.rid, "dns_name_suffix")
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

        self.ipname = data["data"]["ip"]
        self.addr = self.ipname
        self.set_label()
        self.svc._set(self.rid, "ipname", self.ipname)
        if self.gateway in (None, ""):
            gateway = data.get("data", {}).get("network", {}).get("gateway")
            if gateway:
                self.log.info("set gateway=%s", gateway)
                self.svc._set(self.rid, "gateway", gateway)
                self.gateway = gateway
        if self.mask in (None, ""):
            netmask = data.get("data", {}).get("network", {}).get("netmask")
            if netmask:
                self.log.info("set netmask=%s", netmask)
                self.svc._set(self.rid, "netmask", netmask)
                self.mask = str(netmask)
        self.log.info("ip %s allocated", self.ipname)

    def release(self):
        """
        Release an allocated ip a collector managed network.
        """
        from svcBuilder import conf_get_string_scope

        if self.ipname is None:
            self.log.info("skip release: no ipname set")
            return

        try:
            self.getaddr()
        except ex.excError:
            self.log.info("skip release: ipname does not resolve to an address")
            return

        post_data = {}

        try:
            post_data["name"] = conf_get_string_scope(self.svc, self.svc.config,
                                                      self.rid, "dns_name_suffix")
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
            self.log.warning(data["error"])
            return

        if "info" in data:
            self.log.info(data["info"])

        self.svc._unset(self.rid, "ipname")
        self.log.info("ip %s released", self.ipname)


    def provision(self):
        """
        Provision the ip resource, allocate an ip collector's side, and
        start it.
        """
        self.allocate()
        self.start()

    def unprovision(self):
        """
        Unprovision the ip resource, meaning unplumb and release collector's
        side.
        """
        self.stop()
        self.release()


