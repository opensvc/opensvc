#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
# To change this template, choose Tools | Templates
# and open the template in the editor.

import resources as Res
from rcGlobalEnv import *

from rcUtilities import qcall, which
rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)
import socket
import rcStatus
import rcExceptions as ex
import os

class Ip(Res.Resource):
    """ basic ip resource
    """
    def __init__(self, rid=None, ipDev=None, ipName=None, mask=None,
                 optional=False, disabled=False, tags=set([]), always_on=set([])):
        Res.Resource.__init__(self, rid, "ip", optional=optional, disabled=disabled, tags=tags)
        self.ipDev=ipDev
        self.ipName=ipName
        self.mask=mask
        self.label = ipName + '@' + ipDev
        self.always_on = always_on

    def getaddr(self):
        if hasattr(self, 'addr'):
            return
        try:
            a = socket.getaddrinfo(self.ipName, None)
            if len(a) == 0:
                raise Exception
            self.addr = a[0][4][0]
        except:
            if not self.disabled:
                self.log.error("could not resolve name %s"%self.ipName)
                raise ex.excError

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
        except:
            self.status_log("could not resolve %s to an ip address"%self.ipName)
            return rcStatus.WARN
        try:
            if self.is_up():
                return self.status_stdby(rcStatus.UP)
            else:
                return self.status_stdby(rcStatus.DOWN)
        except ex.excNotSupported:
            self.status_log("not supported")
            return rcStatus.UNDEF

    def arp_announce(self):
        if ':' in self.addr:
            return
        if not which("arping"):
            self.log.warning("arp annouce skipped. install 'arping'")
            return
        cmd = ["arping", "-U", "-c", "1", "-I", self.ipDev, "-s", self.addr, "0.0.0.0"]
        self.log.info(' '.join(cmd))
        qcall(cmd)

    def check_ping(self):
        raise ex.MissImpl('check_ping')

    def startip_cmd(self):
        raise ex.MissImpl('startip_cmd')

    def stopip_cmd(self):
        raise ex.MissImpl('stopip_cmd')

    def is_up(self):
        ifconfig = rcIfconfig.ifconfig()
        if ifconfig.has_param("ipaddr", self.addr) is not None or \
           ifconfig.has_param("ip6addr", self.addr) is not None:
            self.log.debug("%s@%s is up" % (self.addr, self.ipDev))
            return True
        self.log.debug("%s@%s is down" % (self.addr, self.ipDev))
        return False

    def allow_start(self):
        ifconfig = rcIfconfig.ifconfig()
        intf = ifconfig.interface(self.ipDev)
        if intf is None or not intf.flag_up:
            self.log.error("Interface %s is not up. Cannot stack over it." % self.ipDev)
            raise ex.IpDevDown(self.ipDev)
        if self.is_up() is True:
            self.log.info("%s is already up on %s" % (self.addr, self.ipDev))
            raise ex.IpAlreadyUp(self.addr)
        if self.check_ping():
            self.log.error("%s is already up on another host" % (self.addr))
            raise ex.IpConflict(self.addr)
        return

    def startstandby(self):
        if rcEnv.nodename in self.always_on:
             self.start()

    def lock(self, timeout=30, delay=1):
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
            self.log.error("unexpected locking error")
            import traceback
            traceback.print_exc()
            raise ex.excError
        self.lockfd = lockfd

    def unlock(self):
        import lock
        lock.unlock(self.lockfd)

    def start(self):
        try:
            self.getaddr()
        except:
            self.log.error("could not resolve %s to an ip address"%self.ipName)
            raise ex.excError
        try:
            self.allow_start()
        except (ex.IpConflict, ex.IpDevDown):
            raise ex.excError
        except ex.IpAlreadyUp:
            return
        self.log.debug('pre-checks passed')

        self.lock()
        ifconfig = rcIfconfig.ifconfig()
        if self.mask is None:
            self.mask = ifconfig.interface(self.ipDev).mask
        if self.mask == '':
            self.log.error("No netmask set on parent interface %s" % self.ipDev)
            raise ex.excError
        elif isinstance(self.mask, list):
            if len(self.mask) > 0:
                self.mask = self.mask[0]
            else:
                self.log.error("No netmask set on parent interface %s" % self.ipDev)
                raise ex.excError
        self.stacked_dev = ifconfig.get_stacked_dev(self.ipDev,\
                                                    self.addr,\
                                                    self.log)
        (ret, out) = self.startip_cmd()
        self.unlock()
        if ret != 0:
            self.log.error("failed")
            raise ex.excError

        self.arp_announce()

    def stop(self):
        try:
            self.getaddr()
        except:
            self.log.error("could not resolve %s to an ip address"%self.ipName)
            raise ex.excError
        if self.is_up() is False:
            self.log.info("%s is already down on %s" % (self.addr, self.ipDev))
            return
        ifconfig = rcIfconfig.ifconfig()
        self.stacked_dev = ifconfig.get_stacked_dev(self.ipDev,\
                                                    self.addr,\
                                                    self.log)
        (ret, out) = self.stopip_cmd()
        if ret != 0:
            self.log.error("failed")
            raise ex.excError

        import time
        tmo = 10
        for i in range(tmo):
            if not self.check_ping():
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

    print """i1=Ip("eth0","192.168.0.173")"""
    i=Ip("eth0","192.168.0.173")
    print "show i", i
    print """i.do_action("start")"""
    i.do_action("start")

