import rcStatus
import resources as Res
import time
import rcExceptions as ex
from rcUtilities import justcall, getaddr, lazy
from rcGlobalEnv import rcEnv

class Container(Res.Resource):
    """
    The container base class.
    """

    def __init__(self,
                 rid,
                 name,
                 guestos=None,
                 type=None,
                 osvc_root_path=None,
                 start_timeout=600,
                 stop_timeout=60,
                 **kwargs):
        Res.Resource.__init__(self,
                              rid=rid,
                              type=type,
                              **kwargs)
        self.start_timeout = start_timeout
        self.stop_timeout = stop_timeout
        self.osvc_root_path = osvc_root_path
        self.sshbin = '/usr/bin/ssh'
        try:
            self.name = name
        except AttributeError:
            # name is a lazy prop of the child class
            pass
        try:
            self.label = name
        except AttributeError:
            # label is a lazy prop of the child class
            pass
        self.guestos = guestos
        if guestos is not None:
            self.guestos = guestos.lower()
        if self.guestos == "windows":
            self.runmethod = None
        else:
            self.runmethod = rcEnv.rsh.split() + [name]
        self.booted = False

    def _info(self):
        """
        Contribute resource key/val pairs to the service's resinfo.
        """
        data = [
            ["name", self.name],
            ["guestos", self.guestos],
        ]
        return data

    @lazy
    def vm_hostname(self):
        try:
            hostname = self.svc.conf_get(self.rid, "hostname")
        except ex.OptNotFound:
            hostname = self.name
        return hostname

    def getaddr(self, cache_fallback=False):
        if hasattr(self, 'addr'):
            return
        if self.type == "container.docker":
            return
        try:
            self.log.debug("resolving %s" % self.vm_hostname)
            self.addr = getaddr(self.vm_hostname, cache_fallback=cache_fallback, log=self.log)
        except Exception as e:
            if not self.is_disabled():
                raise ex.excError("could not resolve name %s: %s" % (self.vm_hostname, str(e)))

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def operational(self):
        if not self.runmethod or not self.svc.has_encap_resources:
            return True
        timeout = 1
        if "ssh" in self.runmethod[0]:
            cmd = [self.sshbin, "-o", "StrictHostKeyChecking=no",
                                "-o", "ForwardX11=no",
                                "-o", "BatchMode=yes",
                                "-n",
                                "-o", "ConnectTimeout="+repr(timeout),
                                 self.name, "pwd"]
        else:
            cmd = self.runmethod + ["pwd"]
        import subprocess
        out, err, ret = justcall(cmd, stdin=self.svc.node.devnull)
        if ret == 0:
            return True
        return False

    def wait_for_startup(self):
        self.wait_for_up()
        self.wait_for_ping()
        self.wait_for_operational()

    def wait_for_up(self):
        self.log.info("wait for up status")
        self.wait_for_fn(self.is_up, self.start_timeout, 2)

    def wait_for_ping(self):
        if hasattr(self, 'ping'):
            self.log.info("wait for container ping")
            self.wait_for_fn(getattr(self, "ping"), self.start_timeout, 2)

    def wait_for_operational(self):
        self.log.info("wait for container operational")
        self.wait_for_fn(self.operational, self.start_timeout, 2)

    def wait_for_shutdown(self):
        self.log.info("wait for down status")
        self.wait_for_fn(self.is_down, self.stop_timeout, 2, errmsg="waited too long for shutdown")

    def install_drp_flag(self):
        print("TODO: install_drp_flag()")

    def where_up(self):
        """ returns None if the vm is not found running anywhere
            or returns the nodename where the vm is found running
        """
        if self.is_up():
            return rcEnv.nodename
        if not hasattr(self, "is_up_on"):
            # to implement in Container child class
            return
        if rcEnv.nodename in self.svc.nodes:
            nodes = self.svc.nodes - set([rcEnv.nodename])
        elif rcEnv.nodename in self.svc.drpnodes:
            nodes = self.svc.drpnodes - set([rcEnv.nodename])
        else:
            nodes = []
        if len(nodes) == 0:
            return
        for node in nodes:
            if getattr(self, "is_up_on")(node):
                return node
        return

    def abort_start_ping(self):
        if self.type == "container.docker":
            # docker container for example
            return False
        try:
            self.getaddr()
            if not hasattr(self, 'addr'):
                Container.getaddr(self)
            if not hasattr(self, 'addr'):
                raise ex.excError()
        except:
            self.log.info("could not resolve %s to an ip address" % self.vm_hostname)
            return True

        u = __import__("rcUtilities"+rcEnv.sysname)
        ping = u.check_ping
        self.log.info("test %s ip %s availability" % (self.name, self.addr))
        if ping(self.addr):
            self.log.info("address %s is alive" % self.addr)
            return True

        return False

    def abort_start(self):
        if self.is_up():
            return False

        if self.abort_start_ping():
            return True

        nodename = self.where_up()
        if nodename is not None and nodename != rcEnv.nodename:
            return True

        return False

    def start(self):
        self.promote_rw()
        self.getaddr()
        where = self.where_up()
        if where is not None:
            self.log.info("container %s already started on %s" % (self.label, where))
            return
        if rcEnv.nodename in self.svc.drpnodes:
            self.install_drp_flag()
        self.create_pg()
        self.container_start()
        self.can_rollback = True
        self.wait_for_startup()
        self.booted = True

    def stop(self):
        self.getaddr(cache_fallback=True)
        if self.is_down():
            self.log.info("container %s already stopped" % self.label)
            return
        self.container_stop()
        try:
            self.wait_for_shutdown()
        except ex.excError:
            self.container_forcestop()
            self.wait_for_shutdown()
        if hasattr(self, "post_container_stop"):
            getattr(self, "post_container_stop")()

    def check_capabilities(self):
        #print("TODO: check_capabilities(self)")
        pass

    def container_start(self):
        print("TODO: container_start(self)")

    def container_stop(self):
        print("TODO: container_stop(self)")

    def container_forcestop(self):
        print("TODO: container_forcestop(self)")

    def check_manual_boot(self):
        print("TODO: check_manual_boot(self)")
        return False

    def is_up(self):
        return False

    def is_down(self):
        return not self.is_up()

    def _status(self, verbose=False):
        if self.pg_frozen():
            return rcStatus.NA
        if not self.check_manual_boot():
            self.status_log("container auto boot is on")
        try:
            self.getaddr()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN
        if not self.check_capabilities():
            self.status_log("insufficient node capabilities")
            return rcStatus.WARN
        if self.is_up():
            return rcStatus.UP
        if self.is_down():
            return rcStatus.DOWN
        else:
            self.status_log("container status is neither up nor down")
            return rcStatus.WARN

    def get_container_info(self):
        print("TODO: get_container_info(self)")
        return {'vcpus': '0', 'vmem': '0'}

    def post_action(self, action):
        if action not in ("stop", "start"):
            return
        self.svc.refresh_ip_status()

    def dns_search(self):
        if self.svc.scaler_slave:
            _svcname = self.svc.svcname[self.svc.svcname.index(".")+1:]
        else:
            _svcname = self.svc.svcname
        elems = (
            "%s.%s.svc.%s" % (_svcname, self.svc.app.lower(), self.svc.cluster_name.lower()),
            "%s.svc.%s" % (self.svc.app.lower(), self.svc.cluster_name.lower()),
            "svc.%s" % self.svc.cluster_name.lower(),
        )
        return elems

