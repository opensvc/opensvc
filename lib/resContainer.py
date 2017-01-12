import rcStatus
import resources as Res
import time
import rcExceptions as ex
from rcUtilities import justcall, getaddr
from rcGlobalEnv import rcEnv
from subprocess import *

class Container(Res.Resource):
    """ in seconds
    """
    startup_timeout = 600
    shutdown_timeout = 60

    def __init__(self,
                 rid,
                 name,
                 guestos=None,
                 type=None,
                 osvc_root_path=None,
                 **kwargs):
        Res.Resource.__init__(self,
                              rid=rid,
                              type=type,
                              **kwargs)
        self.osvc_root_path = osvc_root_path
        self.sshbin = '/usr/bin/ssh'
        self.name = name
        self.label = name
        self.guestos = guestos
        if guestos is not None:
            self.guestos = guestos.lower()
        if self.guestos != "windows":
            self.runmethod = rcEnv.rsh.split() + [name]
        self.booted = False

    def vm_hostname(self):
        if hasattr(self, 'vmhostname'):
            return self.vmhostname
        if self.guestos == "windows":
            self.vmhostname = self.name
            return self.vmhostname
        cmd = self.runmethod + ['hostname']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            self.vmhostname = self.name
        else:
            self.vmhostname = out.strip()
        return self.vmhostname

    def getaddr(self, cache_fallback=False):
        if hasattr(self, 'addr'):
            return
        if len(self.name) == 0:
            # explicitely disabled (ex: docker)
            return
        try:
            self.log.debug("resolving %s" % self.name)
            self.addr = getaddr(self.name, cache_fallback=cache_fallback, log=self.log)
        except Exception as e:
            if not self.disabled:
                raise ex.excError("could not resolve name %s: %s" % (self.name, str(e)))

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def operational(self):
        if self.guestos == "windows":
            """ Windows has no sshd.
            """
            return True
        timeout = 1
        if 'ssh' in self.runmethod[0]:
            cmd = [ self.sshbin, '-o', 'StrictHostKeyChecking=no',
                                 '-o', 'ForwardX11=no',
                                 '-o', 'BatchMode=yes',
                                 '-n',
                                 '-o', 'ConnectTimeout='+repr(timeout),
                                  self.name, 'pwd']
        else:
            cmd = self.runmethod + ['pwd']
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        return False

    def wait_for_startup(self):
        self.log.info("wait for container up status")
        self.wait_for_fn(self.is_up, self.startup_timeout, 2)
        if hasattr(self, 'ping'):
            self.log.info("wait for container ping")
            self.wait_for_fn(self.ping, self.startup_timeout, 2)
        self.log.info("wait for container operational")
        self.wait_for_fn(self.operational, self.startup_timeout, 2)

    def wait_for_shutdown(self):
        self.log.info("wait for container down status")
        for tick in range(self.shutdown_timeout):
            if self.is_down():
                return
            time.sleep(1)
        self.log.error("Waited too long for shutdown")
        raise ex.excError

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
            if self.is_up_on(node):
                return node
        return

    def abort_start_ping(self):
        if len(self.name) == 0:
            # docker container for example
            return False
        try:
            self.getaddr()
            if not hasattr(self, 'addr'):
                Container.getaddr(self)
            if not hasattr(self, 'addr'):
                raise ex.excError()
            u = __import__("rcUtilities"+rcEnv.sysname)
            ping = u.check_ping
            self.log.info("test %s ip %s availability"%(self.name, self.addr))
            if ping(self.addr):
                return True
        except:
            self.log.info("could not resolve %s to an ip address. skip ip availability test."%self.name)

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
            self.post_container_stop()

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
            return self.status_stdby(rcStatus.UP)
        if self.is_down():
            return self.status_stdby(rcStatus.DOWN)
        else:
            self.status_log("container status is neither up nor down")
            return rcStatus.WARN

    def get_container_info(self):
        print("TODO: get_container_info(self)")
        return {'vcpus': '0', 'vmem': '0'}
