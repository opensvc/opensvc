"""
Generic ip resource driver.
"""
from __future__ import unicode_literals

import os
import re
import time

import core.status
import utilities.net.ipaddress
import utilities.lock
import core.exceptions as ex
import utilities.ifconfig

from core.objects.svcdict import KEYS
from core.resource import Resource
from env import Env
from utilities.converters import convert_duration, print_duration
from utilities.lazy import lazy
from utilities.loop_delay import delay
from utilities.net.converters import to_cidr
from utilities.net.getaddr import getaddr

KW_IPNAME = {
    "keyword": "ipname",
    "required": False,
    "at": True,
    "text": "The DNS name or IP address of the ip resource. Can be different from one node to the other, in which case ``@nodename`` can be specified. This is most useful to specify a different ip when the service starts in DRP mode, where subnets are likely to be different than those of the production datacenter. With the amazon driver, the special ``<allocate>`` value tells the provisioner to assign a new private address."
}
KW_IPDEV = {
    "keyword": "ipdev",
    "at": True,
    "required": True,
    "text": "The interface name over which OpenSVC will try to stack the service ip. Can be different from one node to the other, in which case the ``@nodename`` can be specified."
}
KW_NETMASK = {
    "keyword": "netmask",
    "at": True,
    "text": "If an ip is already plumbed on the root interface (in which case the netmask is deduced from this ip). Mandatory if the interface is dedicated to the service (dummy interface are likely to be in this case). The format is either dotted or octal for IPv4, ex: 255.255.252.0 or 22, and octal for IPv6, ex: 64.",
    "example": "255.255.255.0"
}
KW_GATEWAY = {
    "keyword": "gateway",
    "at": True,
    "text": "A zone ip provisioning parameter used in the sysidcfg formatting. The format is decimal for IPv4, ex: 255.255.252.0, and octal for IPv6, ex: 64.",
    "provisioning": True
}
KW_DNS_NAME_SUFFIX = {
    "keyword": "dns_name_suffix",
    "at": True,
    "text": "Add the value as a suffix to the DNS record name. The record created is thus formatted as ``<name>-<dns_name_suffix>.<app>.<managed zone>``."
}
KW_PROVISIONER = {
    "keyword": "provisioner",
    "provisioning": True,
    "candidates": ("collector", None),
    "at": True,
    "example": "collector",
    "text": "The IPAM driver to use to provision the ip.",
}
KW_NETWORK = {
    "keyword": "network",
    "at": True,
    "example": "10.0.0.0",
    "text": "The network, in dotted notation, from where the ip provisioner allocates. Also used by the docker ip driver to delete the network route if :kw:`del_net_route` is set to ``true``.",
}
KW_DNS_UPDATE = {
    "keyword": "dns_update",
    "at": True,
    "default": False,
    "convert": "boolean",
    "candidates": [True, False],
    "text": "Setting this parameter triggers a DNS update. The record created is formatted as ``<name>.<app>.<managed zone>``, unless dns_record_name is specified."
}
KW_WAIT_DNS = {
    "section": "ip",
    "keyword": "wait_dns",
    "at": True,
    "convert": "duration",
    "default": 0,
    "example": "10s",
    "text": "Wait for the cluster DNS records associated to the resource to appear after a resource start and before the next resource can be started. This can be used for apps or containers that require the ip or ip name to be resolvable to provision or execute properly."
}
KW_ZONE = {
    "keyword": "zone",
    "at": True,
    "text": "The zone name the ip resource is linked to. If set, the ip is plumbed from the global in the zone context.",
    "example": "zone1"
}
KW_CHECK_CARRIER = {
    "keyword": "check_carrier",
    "at": True,
    "required": False,
    "default": True,
    "convert": "boolean",
    "text": "Activate the link carrier check. Set to false if ipdev is a backend "
            "bridge or switch",
}
KW_ALIAS = {
    "keyword": "alias",
    "at": True,
    "required": False,
    "default": True,
    "convert": "boolean",
    "text": "Use ip aliasing. Modern ip stack support multiple ip/mask per interface, so :kw:`alias` should be set to false when possible.",
}
KW_EXPOSE = {
    "keyword": "expose",
    "at": True,
    "required": False,
    "default": [],
    "convert": "list",
    "text": "A whitespace-separated list of ``<port>/<protocol>[:<host port>]`` "
                   "describing socket services that mandate a SRV exposition. With "
                   "<host_port> set, the ip.cni driver configures port mappings too.",
    "example": "443/tcp:8443 53/udp"
}

COMMON_KEYWORDS = [
    KW_WAIT_DNS,
    KW_DNS_NAME_SUFFIX,
    KW_PROVISIONER,
    KW_NETWORK,
    KW_DNS_UPDATE,
    KW_CHECK_CARRIER,
    KW_ALIAS,
    KW_EXPOSE,
]

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "host"
KEYWORDS = [
    KW_IPNAME,
    KW_IPDEV,
    KW_NETMASK,
    KW_GATEWAY,
] + COMMON_KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class Ip(Resource):
    """
    Base ip resource driver.
    """

    def __init__(self,
                 ipdev=None,
                 ipname=None,
                 netmask=None,
                 gateway=None,
                 type="ip",
                 expose=None,
                 check_carrier=True,
                 alias=True,
                 wait_dns=0,
                 provisioner=None,
                 **kwargs):
        super(Ip, self).__init__(type=type, **kwargs)
        self.ipdev = ipdev
        self.ipname = ipname
        self.netmask = netmask
        self.gateway = gateway
        self.lockfd = None
        self.stacked_dev = None
        self.addr = None
        self.expose = expose
        self.check_carrier = check_carrier
        self.alias = alias
        self.wait_dns = wait_dns
        self.kw_provisioner = provisioner

    def on_add(self):
        self.set_label()

    def wait_dns_records(self):
        if not self.wait_dns:
            return

        # refresh the ipaddr advertized in status.json
        self.status_info()
        self.write_status_last()
        self.svc.print_status_data_eval()

        left = self.wait_dns
        time_max = self._current_time() + left
        self.log.info("wait address propagation to peers (wait_dns=%s)", print_duration(left))
        path = ".monitor.nodes.'%s'.services.status.'%s'.resources.'%s'.info.ipaddr~[0-9]" % (Env.nodename, self.svc.path, self.rid)
        try:
            result = self.svc.node._wait(path=path, duration=left)
        except KeyboardInterrupt:
            raise ex.Error("dns resolution not ready after %s (ip not in local dataset)" % print_duration(self.wait_dns))
        left = time_max - self._current_time()
        self.log.info("wait cluster sync (time left is %s)", print_duration(left))
        while left > 0:
            result = self.svc.node.daemon_get({
                "action": "sync",
                "options": {
                    "timeout": left
                },
            }, timeout=left+10)
            if result["status"] == 0:
                return
            wait_dns_records_delay_func(0.3)  # avoid fast-looping the listener
            left = time_max - self._current_time()
        raise ex.Error("dns resolution not ready after %s (cluster sync timeout)" % print_duration(self.wait_dns))

    @lazy
    def dns_name_suffix(self):
        try:
            dns_name_suffix = self.conf_get("dns_name_suffix").strip("'\"$#")
        except ex.OptNotFound:
            dns_name_suffix = None
        return dns_name_suffix

    def set_label(self):
        """
        Set the resource label property.
        """
        try:
             self.get_mask()
        except ex.Error:
             pass
        try:
            self.getaddr()
            addr = self.addr
        except ex.Error:
            addr = self.ipname
        self.label = "%s/%s %s" % (addr, self.netmask, self.ipdev)
        if self.ipname != addr:
            self.label += " " + self.ipname

    def _status_info(self):
        """
        Contribute resource key/val pairs to the resource info.
        """
        data = {}
        try:
            self.getaddr()
        except ex.Error:
            pass

        try:
            data["ipaddr"] = self.addr
        except:
            pass

        if self.ipdev:
            data["ipdev"] = self.ipdev
        if self.gateway:
            data["gateway"] = self.gateway
        if self.netmask is not None:
            data["netmask"] = to_cidr(self.netmask)
        if self.expose:
            data["expose"] = self.expose
        return data

    def _info(self):
        """
        Contribute resource key/val pairs to the service's resinfo.
        """
        if self.ipname is None:
            return []
        try:
            self.getaddr()
        except ex.Error:
            pass
        data = [
            ["ipaddr", self.addr],
            ["ipname", self.ipname],
            ["ipdev", self.ipdev],
            ["gateway", str(self.gateway)],
        ]
        if self.netmask is not None:
            data.append(["netmask", str(to_cidr(self.netmask))])
        return data

    def getaddr(self, cache_fallback=False):
        """
        Try resolving the ipname into an ip address. If the resolving fails and
        <cache_fallback> is True, use the last successful resolution result.
        """
        if self.ipname is None:
            raise ex.Error("ip address is not allocated yet")
        if self.addr is not None:
            return
        try:
            self.log.debug("resolving %s", self.ipname)
            self.addr = getaddr(self.ipname, cache_fallback=cache_fallback, log=self.log)
        except Exception as exc:
            if not self.is_disabled():
                raise ex.Error("could not resolve name %s: %s" % (self.ipname, str(exc)))

    def __str__(self):
        return "%s ipdev=%s ipname=%s" % (super(Ip, self).__str__(), \
                                         self.ipdev, self.ipname)

    def setup_environ(self):
        """
        Set the main resource properties as environment variables, so they
        are available to triggers.
        """
        os.environ['OPENSVC_IPDEV'] = str(self.ipdev)
        os.environ['OPENSVC_IPNAME'] = str(self.ipname)
        os.environ['OPENSVC_NETMASK'] = str(self.netmask)
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
        for prop in ['ipname', 'ipdev', 'addr', 'netmask']:
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
            if "not allocated" in str(exc):
                return core.status.DOWN
            else:
                return core.status.WARN
        ifconfig = self.get_ifconfig()
        intf = ifconfig.interface(self.ipdev)
        mode = getattr(self, "mode") if hasattr(self, "mode") else None
        if intf is None and "dedicated" not in self.tags and mode != "dedicated":
            self.status_log("interface %s not found" % self.ipdev)
            return core.status.DOWN
        try:
            if self.is_up() and self.has_carrier(intf) is not False:
                return core.status.UP
            else:
                return core.status.DOWN
        except ex.NotSupported:
            self.status_log("not supported", "info")
            return core.status.NA
        except ex.Error as exc:
            self.status_log(str(exc), "error")
            return core.status.WARN

    def arp_announce(self):
        """
        Announce to neighbors the ip address is plumbed on ipdev through a
        arping broadcast of unsollicited packets.
        """
        if ':' in self.addr or self.ipdev in ("lo", "lo0"):
            return
        self.log.info("send gratuitous arp to announce %s is at %s", self.addr, self.ipdev)
        from utilities.arp import send_arp
        send_arp(self.ipdev, self.addr)

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
        return self._is_up(ifconfig)

    def has_carrier(self, intf):
        if intf is None:
            return
        if not self.check_carrier:
            return
        mode = getattr(self, "mode") if hasattr(self, "mode") else None
        if "dedicated" in self.tags or mode == "dedicated":
            return
        if intf.flag_no_carrier:
            self.status_log("no carrier")
            return False
        return True

    def _is_up(self, ifconfig):
        intf = ifconfig.has_param("ipaddr", self.addr)
        if intf is not None:
            if isinstance(intf.ipaddr, list):
                idx = intf.ipaddr.index(self.addr)
                current_mask = to_cidr(intf.mask[idx])
            else:
                current_mask = to_cidr(intf.mask)
            if self.netmask is None:
                self.status_log("netmask is not set nor guessable")
            elif current_mask != to_cidr(self.netmask):
                self.status_log("current netmask %s, expected %s" %
                                (current_mask, to_cidr(self.netmask)))
            ref_dev = intf.name.split(":")[0]
            if self.type == "ip" and ref_dev != self.ipdev:
                self.status_log("current dev %s, expected %s" %
                                (ref_dev, self.ipdev))
            return True
        intf = ifconfig.has_param("ip6addr", self.addr)
        if intf is not None:
            if isinstance(intf.ip6addr, list):
                idx = intf.ip6addr.index(self.addr)
                current_mask = to_cidr(intf.ip6mask[idx])
            else:
                current_mask = to_cidr(intf.ip6mask)
            if current_mask != to_cidr(self.netmask):
                self.status_log("current netmask %s, expected %s" %
                                (current_mask, to_cidr(self.netmask)))
            ref_dev = intf.name.split(":")[0]
            if self.type == "ip" and ref_dev != self.ipdev:
                self.status_log("current dev %s, expected %s" %
                                (ref_dev, self.ipdev))
            return True
        return False

    def allow_start(self):
        """
        Do sanity checks before allowing the start.
        """
        if self.is_up() is True:
            self.log.info("%s is already up on %s", self.addr, self.ipdev)
            raise ex.IpAlreadyUp(self.addr)
        ifconfig = self.get_ifconfig()
        intf = ifconfig.interface(self.ipdev)
        if self.has_carrier(intf) is False and not self.svc.options.force:
            self.log.error("interface %s no-carrier.", self.ipdev)
            raise ex.IpDevDown(self.ipdev)
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
        timeout = convert_duration(self.svc.options.waitlock)
        if timeout is None or timeout < 0:
            timeout = 120
        delay = 1
        lockfd = None
        action = "startip"
        lockfile = os.path.join(Env.paths.pathlock, action)
        details = "(timeout %d, delay %d, action %s, lockfile %s)" % \
                  (timeout, delay, action, lockfile)
        self.log.debug("acquire startip lock %s", details)

        try:
            lockfd = utilities.lock.lock(timeout=timeout, delay=delay, lockfile=lockfile, intent="startip")
        except utilities.lock.LockTimeout as exc:
            raise ex.Error("timed out waiting for lock %s: %s" % (details, str(exc)))
        except utilities.lock.LockNoLockFile:
            raise ex.Error("lock_nowait: set the 'lockfile' param %s" % details)
        except utilities.lock.LockCreateError:
            raise ex.Error("can not create lock file %s" % details)
        except utilities.lock.LockAcquire as exc:
            raise ex.Error("another action is currently running %s: %s" % (details, str(exc)))
        except ex.Signal:
            raise ex.Error("interrupted by signal %s" % details)
        except Exception as exc:
            self.save_exc()
            raise ex.Error("unexpected locking error %s: %s" % (details, str(exc)))

        if lockfd is not None:
            self.lockfd = lockfd

    def unlock(self):
        """
        Release the startip lock.
        """
        utilities.lock.unlock(self.lockfd)

    @staticmethod
    def get_ifconfig():
        """
        Wrapper around the os specific rcIfconfig module's ifconfig function.
        Return a parsed ifconfig dataset.
        """
        return utilities.ifconfig.Ifconfig()

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
            raise ex.Error
        except (ex.IpAlreadyUp, ex.IpNoActions):
            return
        self.log.debug('pre-checks passed')

        self.lock()
        try:
            arp_announce = self.start_locked()
        finally:
            self.unlock()

        if arp_announce:
            try:
                self.arp_announce()
            except AttributeError:
                self.log.info("arp announce not supported")

        try:
            self.dns_update()
        except ex.Error as exc:
            self.log.error(str(exc))
        self.wait_dns_records()

    def get_mask(self, ifconfig=None):
        if ifconfig is None:
            ifconfig = self.get_ifconfig()
        if self.netmask is None:
            intf = ifconfig.interface(self.ipdev)
            if intf is None:
                raise ex.Error("netmask parameter is mandatory with 'noalias' tag")
            self.netmask = intf.mask
        if not self.netmask:
            if "noaction" not in self.tags:
                self.netmask = None
                raise ex.Error("No netmask set on parent interface %s" % self.ipdev)
        if isinstance(self.netmask, list):
            try:
                self.netmask = self.netmask[0]
            except IndexError:
                self.netmask = None

    def start_locked(self):
        """
        The start codepath fragment protected by the startip lock.
        """
        self.get_stack_dev()
        if self.stacked_dev is None:
            raise ex.Error("could not determine a stacked dev for parent "
                           "interface %s" % self.ipdev)

        arp_announce = True
        try:
            ret = self.startip_cmd()[0]
            self.can_rollback = True
        except ex.NotSupported:
            self.log.info("start ip not supported")
            ret = 0
            arp_announce = False

        if ret != 0:
            raise ex.Error("failed")

        return arp_announce

    def get_stack_dev(self):
        ifconfig = self.get_ifconfig()
        self.get_mask(ifconfig)
        if 'noalias' in self.tags:
            self.stacked_dev = self.ipdev
        else:
            self.stacked_dev = ifconfig.get_stacked_dev(self.ipdev,
                                                        self.addr,
                                                        self.log)

    def dns_update(self):
        """
        Post a dns update request to the collector.
        """
        if self.svc.node.collector_env.dbopensvc is None:
            return

        if self.ipname is None:
            self.log.debug("skip dns update: ipname is not set")
            return

        try:
            self.conf_get("dns_update")
        except ex.OptNotFound:
            self.log.debug("skip dns update: dns_update is not set")
            return

        if not self.is_up():
            self.log.debug("skip dns update: resource is not up")
            return

        if self.dns_name_suffix is None:
            self.log.debug("dns update: dns_name_suffix is not set")

        try:
            self.getaddr()
        except ex.Error as exc:
            self.log.error(str(exc))
            return

        post_data = {
            "content": self.addr,
        }

        if self.dns_name_suffix:
            post_data["name"] = self.dns_name_suffix

        try:
            data = self.svc.node.collector_rest_post(
                "/dns/services/records",
                post_data,
                path=self.dns_rec_name(),
            )
        except Exception as exc:
            raise ex.Error("dns update failed: "+str(exc))
        if "error" in data:
            raise ex.Error(data["error"])

        self.log.info("dns updated")

    def dns_rec_name(self):
        return self.svc.path

    def stop(self):
        """
        Stop the resource.
        """
        if self.ipname is None:
            self.log.info("skip stop: no ipname set")
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
            raise ex.Error

        try:
            ret = self.stopip_cmd()[0]
        except ex.NotSupported:
            self.log.info("stop ip not supported")
            return

        if ret != 0:
            self.log.error("failed")
            raise ex.Error

        tmo = 15
        idx = 0
        for idx in range(tmo):
            if not self.check_ping(count=1, timeout=1):
                break
            time.sleep(1)

        if idx == tmo-1:
            self.log.error("%s refuse to go down", self.addr)
            raise ex.Error

    def allocate(self):
        """
        Request an ip in the ipdev network from the collector.
        """
        if self.svc.node.collector_env.dbopensvc is None:
            return

        try:
            self.conf_get("ipname")
            self.log.info("skip allocate: an ip is already defined")
            return
        except ex.RequiredOptNotFound:
            pass
        except ex.OptNotFound:
            pass

        if self.ipdev is None:
            self.log.info("skip allocate: ipdev is not set")
            return

        try:
            # explicit network setting
            network = self.conf_get("network")
        except ex.OptNotFound:
            network = None

        if network is None:
            # implicit network: the network of the first ipdev ip
            ifconfig = self.get_ifconfig()
            intf = ifconfig.interface(self.ipdev)
            try:
                if isinstance(intf.ipaddr, list):
                    baseaddr = intf.ipaddr[0]
                else:
                    baseaddr = intf.ipaddr
                network = str(utilities.net.ipaddress.IPv4Interface(baseaddr).network.network_address)
            except (ValueError, IndexError):
                self.log.info("skip allocate: ipdev %s has no configured address "
                              "and network is not set", self.ipdev)
                return

        post_data = {
            "network": network,
        }

        if self.dns_name_suffix:
            post_data["name"] = self.dns_name_suffix
        else:
            self.log.debug("allocate: dns_name_suffix is not set")

        try:
            data = self.svc.node.collector_rest_post(
                "/networks/%s/allocate" % network,
                post_data,
                path=self.dns_rec_name(),
            )
        except Exception as exc:
            raise ex.Error("ip allocation failed: "+str(exc))
        if "error" in data:
            raise ex.Error(data["error"])

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
        if self.netmask in (None, ""):
            netmask = data.get("data", {}).get("network", {}).get("netmask")
            if netmask:
                self.log.info("set netmask=%s", netmask)
                self.svc._set(self.rid, "netmask", netmask)
                self.netmask = str(netmask)
        self.log.info("ip %s allocated", self.ipname)
        record_name = data["data"].get("record_name")
        if record_name:
            self.log.info("record %s created", record_name)

    def release(self):
        """
        Release an allocated ip a collector managed network.
        """
        if self.svc.node.collector_env.dbopensvc is None:
            return

        if self.ipname is None:
            self.log.info("skip release: no ipname set")
            return

        try:
            self.getaddr()
        except ex.Error:
            self.log.info("skip release: ipname does not resolve to an address")
            return

        post_data = {}

        if self.dns_name_suffix:
            post_data["name"] = self.dns_name_suffix
        else:
            self.log.debug("allocate: dns_name_suffix is not set")

        try:
            data = self.svc.node.collector_rest_post(
                "/networks/%s/release" % self.addr,
                post_data,
                path=self.dns_rec_name(),
            )
        except Exception as exc:
            raise ex.Error("ip release failed: "+str(exc))
        if "error" in data:
            self.log.warning(data["error"])
            return

        if "info" in data:
            self.log.info(data["info"])

        self.svc.unset_multi(["%s.ipname" % self.rid])
        self.log.info("ip %s released", self.ipname)

    def expose_data(self):
        if self.expose is None:
            return []
        data = []
        for expose in self.expose:
            data.append(self.parse_expose(expose))
        return data

    def parse_expose(self, expose):
        data = {}
        if "#" in expose:
           # expose data via reference
           resource = self.svc.get_resource(expose)
           data["port"] = resource.options.port
           data["protocol"] = resource.options.protocol
           try:
               data["host_port"] = resource.options.host_port
           except AttributeError:
               pass
           return data

        # expose data inline
        words = expose.split(":")
        if len(words) == 2:
            try:
                data["host_port"] = int(words[1])
            except ValueError:
                raise ex.Error("invalid host port format %s. expected integer" % words[1])
        words = re.split("[-/]", words[0])
        if len(words) != 2:
            raise ex.Error("invalid expose format %s. expected <nsport>/<proto>[:<hostport>]" % expose)
        try:
            data["port"] = int(words[0])
        except ValueError:
            raise ex.Error("invalid expose port format %s. expected integer" % words[0])
        if words[1] not in ("tcp", "udp"):
            raise ex.Error("invalid expose protocol %s. expected tcp or udp" % words[1])
        data["protocol"] = words[1]
        return data

    def provisioned(self):
        try:
            self.conf_get("ipname")
            return True
        except ex.OptNotFound:
            return False

    def provisioner(self):
        """
        Provision the ip resource, allocate an ip collector's side, and
        start it.
        """
        if self.kw_provisioner != "collector":
            return
        self.allocate()

    def unprovisioner(self):
        """
        Unprovision the ip resource, meaning unplumb and release collector's
        side.
        """
        if self.kw_provisioner != "collector":
            return
        self.release()

    def _current_time(self):
        return time.time()


# helper for mock
wait_dns_records_delay_func = delay