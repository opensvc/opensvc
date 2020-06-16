import core.status
import core.exceptions as ex
import utilities.ping

from env import Env
from utilities.lazy import lazy
from core.resource import Resource
from utilities.proc import justcall
from utilities.net.getaddr import getaddr

KW_START_TIMEOUT = {   
    "keyword": "start_timeout",
    "convert": "duration",
    "at": True,
    "text": "Wait for <duration> before declaring the container action a failure.",
    "default": "240",
    "example": "180"
}
KW_STOP_TIMEOUT = {   
    "keyword": "stop_timeout",
    "convert": "duration",
    "at": True,
    "text": "Wait for <duration> before declaring the container action a failure.",
    "default": "120",
    "example": "180"
}
KW_SNAP = {
    "keyword": "snap",
    "text": "The target snapshot/clone full path containing the new container disk files.",
    "required": False,
    "provisioning": True
}
KW_SNAPOF = {
    "keyword": "snapof",
    "text": "The snapshot origin full path containing the reference container disk files.",
    "required": False,
    "provisioning": True
}
KW_VIRTINST = {
    "keyword": "virtinst",
    "text": "The :cmd:`virt-install` command to use to create the container.",
    "convert": "shlex",
    "required": True,
    "provisioning": True
}
KW_NO_PREEMPT_ABORT = {
    "keyword": "no_preempt_abort",
    "at": True,
    "candidates": (True, False),
    "default": False,
    "convert": "boolean",
    "text": "If set to ``true``, OpenSVC will preempt scsi reservation with a preempt command instead of a preempt and and abort. Some scsi target implementations do not support this last mode (esx). If set to ``false`` or not set, :kw:`no_preempt_abort` can be activated on a per-resource basis."
}
KW_NAME = {
    "keyword": "name",
    "at": True,
    "default_text": "The container name.",
    "text": "Set if the container hostname is different from the container name."
}
KW_HOSTNAME = {
    "keyword": "hostname",
    "at": True,
    "text": "This need to be set if the virtual machine hostname is different from the machine name."
}
KW_OSVC_ROOT_PATH = {
    "keyword": "osvc_root_path",
    "at": True,
    "example": "/opt/opensvc",
    "text": "If the OpenSVC agent is installed via package in the container, this parameter must not be set. Else the value can be set to the fullpath hosting the agent installed from sources."
}
KW_GUESTOS = {
    "keyword": "guestos",
    "at": True,
    "candidates": ["unix", "windows"],
    "text": "The operating system in the virtual machine."
}
KW_SHARED_IP_GROUP = {
    "keyword": "shared_ip_group",
    "at": True,
    "text": "The cloud shared ip group name to allocate a public ip from."
}
KW_SIZE = {
    "keyword": "size",
    "at": True,
    "text": "The cloud vm size, as known to the cloud manager.",
    "example": "tiny"
}
KW_KEY_NAME = {
    "keyword": "key_name",
    "at": True,
    "required": True,
    "text": "The key name, as known to the cloud manager, to trust in the provisioned vm."
}
KW_CLOUD_ID = {
    "keyword": "cloud_id",
    "required": True,
    "at": True,
    "text": "The cloud id as configured in ``node.conf``.",
    "example": "cloud#1"
}
KW_PROMOTE_RW = {
    "keyword": "promote_rw",
    "default": False,
    "convert": "boolean",
    "candidates": (True, False),
    "text": "If set to ``true``, OpenSVC will try to promote the base devices to read-write on start."
}
KW_SCSIRESERV = {
    "keyword": "scsireserv",
    "default": False,
    "convert": "boolean",
    "candidates": (True, False),
    "text": "If set to ``true``, OpenSVC will try to acquire a type-5 (write exclusive, registrant only) scsi3 persistent reservation on every path to every disks held by this resource. Existing reservations are preempted to not block service start-up. If the start-up was not legitimate the data are still protected from being written over from both nodes. If set to ``false`` or not set, :kw:`scsireserv` can be activated on a per-resource basis."
}


class BaseContainer(Resource):
    """
    The container base class.
    """

    def __init__(self,
                 name=None,
                 guestos=None,
                 type=None,
                 osvc_root_path=None,
                 start_timeout=600,
                 stop_timeout=60,
                 **kwargs):
        super(BaseContainer, self).__init__(type=type, **kwargs)
        self.start_timeout = start_timeout
        self.stop_timeout = stop_timeout
        self.osvc_root_path = osvc_root_path
        self.sshbin = '/usr/bin/ssh'
        self.raw_name = name
        self.guestos = guestos
        if guestos is not None:
            self.guestos = guestos.lower()
        self.booted = False

    @lazy
    def name(self):
        return self.raw_name or self.svc.name

    @lazy
    def label(self):  # pylint: disable=method-hidden
        return self.name

    @lazy
    def runmethod(self):
        if self.guestos == "windows":
            return
        return Env.rsh.split() + [self.name]

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
            hostname = self.conf_get("hostname")
        except ex.OptNotFound:
            hostname = self.name
        return hostname

    def getaddr(self, cache_fallback=False):
        if hasattr(self, 'addr'):
            return
        try:
            self.log.debug("resolving %s" % self.vm_hostname)
            self.addr = getaddr(self.vm_hostname, cache_fallback=cache_fallback, log=self.log)
        except Exception as e:
            if not self.is_disabled():
                raise ex.Error("could not resolve name %s: %s" % (self.vm_hostname, str(e)))

    def __str__(self):
        return "%s name=%s" % (super(BaseContainer, self).__str__(), self.name)

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
        out, err, ret = justcall(cmd, stdin=self.svc.node.devnull)
        if ret == 0:
            return True
        return False

    def wait_for_startup(self):
        self.wait_for_up()
        self.wait_for_ping()
        self.wait_for_operational()

    def wait_for_up(self):
        def fn():
            if hasattr(self, "is_up_clear_cache"):
                getattr(self, "is_up_clear_cache")()
            return self.is_up()
        self.log.info("wait for up status")
        self.wait_for_fn(fn, self.start_timeout, 2)

    def wait_for_ping(self):
        """
        Wait for container to become alive, using a ping test.
        Also verify the container has not died since judged started.
        """
        def fn():
            if hasattr(self, "is_up_clear_cache"):
                getattr(self, "is_up_clear_cache")()
            if not self.is_up():
                raise ex.Error("the container went down")
            return getattr(self, "ping")()
        if hasattr(self, 'ping'):
            self.log.info("wait for container ping")
            self.wait_for_fn(fn, self.start_timeout, 2)

    def wait_for_operational(self):
        """
        Wait for container to become operational, using a driver-specific
        test (usually ssh).
        Also verify the container has not died since judged started.
        """
        def fn():
            if hasattr(self, "is_up_clear_cache"):
                getattr(self, "is_up_clear_cache")()
            if not self.is_up():
                raise ex.Error("the container went down")
            return self.operational()
        self.log.info("wait for container operational")
        self.wait_for_fn(fn, self.start_timeout, 2)

    def wait_for_shutdown(self):
        def fn():
            if hasattr(self, "is_up_clear_cache"):
                getattr(self, "is_up_clear_cache")()
            return not self.is_up()
        self.log.info("wait for down status")
        self.wait_for_fn(fn, self.stop_timeout, 2, errmsg="waited too long for shutdown")

    def install_drp_flag(self):
        print("TODO: install_drp_flag()")

    def where_up(self):
        """ returns None if the vm is not found running anywhere
            or returns the nodename where the vm is found running
        """
        if self.is_up():
            return Env.nodename
        if not hasattr(self, "is_up_on"):
            # to implement in Container child class
            return
        if Env.nodename in self.svc.nodes:
            nodes = self.svc.nodes - set([Env.nodename])
        elif Env.nodename in self.svc.drpnodes:
            nodes = self.svc.drpnodes - set([Env.nodename])
        else:
            nodes = []
        if len(nodes) == 0:
            return
        for node in nodes:
            if getattr(self, "is_up_on")(node):
                return node
        return

    def abort_start_ping(self):
        if self.svc.get_resources("ip"):
            # we manage an ip, no need to try to ping the container
            return False
        try:
            self.getaddr()
            if not hasattr(self, 'addr'):
                BaseContainer.getaddr(self)
            if not hasattr(self, 'addr'):
                raise ex.Error()
        except:
            self.log.info("could not resolve %s to an ip address" % self.vm_hostname)
            return True

        self.log.info("test %s ip %s availability" % (self.name, self.addr))
        if utilities.ping.check_ping(self.addr):
            self.log.info("address %s is alive" % self.addr)
            return True

        return False

    def abort_start(self):
        if self.is_up():
            return False

        if self.abort_start_ping():
            return True

        nodename = self.where_up()
        if nodename is not None and nodename != Env.nodename:
            return True

        return False

    def start(self):
        self.promote_rw()
        self.getaddr()
        where = self.where_up()
        self.create_pg()
        if where is not None:
            self.log.info("container %s already started on %s" % (self.label, where))
            return
        if Env.nodename in self.svc.drpnodes:
            self.install_drp_flag()
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
        except ex.Error:
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
            return core.status.NA
        if not self.check_manual_boot():
            self.status_log("container auto boot is on")
        try:
            self.getaddr()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN
        if not self.check_capabilities():
            self.status_log("insufficient node capabilities")
            return core.status.WARN
        if self.is_up():
            return core.status.UP
        if self.is_down():
            return core.status.DOWN
        else:
            self.status_log("container status is neither up nor down")
            return core.status.WARN

    def get_container_info(self):
        print("TODO: get_container_info(self)")
        return {'vcpus': '0', 'vmem': '0'}

    def post_action(self, action):
        if action not in ("stop", "start"):
            return
        self.svc.refresh_ip_status()

    def dns_search(self):
        if self.svc.scaler_slave:
            try:
                _name = self.svc.name[self.svc.name.index(".")+1:]
            except ValueError:
                raise ex.Error("misnamed scaler slave %s: should be <n>.<scalername>" % self.svc.name)
        else:
            _name = self.svc.name
        namespace = self.svc.namespace.lower() if self.svc.namespace else "root"
        elems = (
            "%s.%s.svc.%s" % (_name, namespace, self.svc.cluster_name.lower()),
            "%s.svc.%s" % (namespace, self.svc.cluster_name.lower()),
            "svc.%s" % self.svc.cluster_name.lower(),
        )
        return elems


    def dns_options(self, options):
        ndots_done = False
        edns0_done = False
        usevc_done = False
        for co, i in enumerate(options):
            try:
                if co.startswith("ndots:"):
                    ndots = int(co.replace("ndots:", ""), "")
                    if ndots < 2:
                        options[i] = "ndots:2"
                    ndots_done = True
            except Exception:
                pass
            if co == "edns0":
                edns0_done = True
            elif co == "use-vc":
                usevc_done = True
        if not ndots_done:
            options.append("ndots:2")
        if not edns0_done:
            options.append("edns0")
        if not usevc_done:
            options.append("use-vc")
        return options

