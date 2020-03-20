import os
import time

import rcExceptions as ex
import rcStatus
import resContainer
import resources as Res

from rcGlobalEnv import rcEnv
from rcUtilities import justcall, lazy
from rcUtilitiesLinux import check_ping
from svcdict import KEYS
from svcBuilder import init_kwargs, container_kwargs


DRIVER_GROUP = "container"
DRIVER_BASENAME = "amazon"
KEYWORDS = [
    resContainer.KW_START_TIMEOUT,
    resContainer.KW_STOP_TIMEOUT,
    resContainer.KW_NO_PREEMPT_ABORT,
    resContainer.KW_NAME,
    resContainer.KW_HOSTNAME,
    resContainer.KW_OSVC_ROOT_PATH,
    resContainer.KW_GUESTOS,
    resContainer.KW_SHARED_IP_GROUP,
    resContainer.KW_SIZE,
    resContainer.KW_KEY_NAME,
    resContainer.KW_CLOUD_ID,
    resContainer.KW_PROMOTE_RW,
    resContainer.KW_SCSIRESERV,
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["cloud_id"] = svc.oget(s, "cloud_id")
    kwargs["key_name"] = svc.oget(s, "key_name")

    # provisioning keywords
    kwargs["image_id"] = svc.oget(s, "image_id")
    kwargs["size"] = svc.oget(s, "size")
    kwargs["subnet"] = svc.oget(s, "subnet")
    r = CloudVm(**kwargs)
    svc += r

class CloudVm(resContainer.Container):
    save_timeout = 240

    def __init__(self,
                 rid,
                 name,
                 guestos=None,
                 cloud_id=None,
                 image_id=None,
                 size="t2.micro",
                 key_name=None,
                 subnet=None,
                 osvc_root_path=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.amazon",
                                        guestos=guestos,
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self.cloud_id = cloud_id
        self.save_name = name + '.save'
        self.size_id = size
        self.image_id = image_id
        self.key_name = key_name
        self.subnet_name = subnet
        self.addr = None

    def keyfile(self):
        kf = [os.path.join(rcEnv.paths.pathetc, self.key_name+'.pem'),
              os.path.join(rcEnv.paths.pathetc, self.key_name+'.pub'),
              os.path.join(rcEnv.paths.pathvar, self.key_name+'.pem'),
              os.path.join(rcEnv.paths.pathvar, self.key_name+'.pub')]
        for k in kf:
            if os.path.exists(k):
                return k
        raise ex.excError("key file for key name '%s' not found"%self.key_name)

    def rcp_from(self, src, dst):
        if self.guestos == "windows":
            """ Windows has no sshd.
            """
            raise ex.excNotSupported("remote copy not supported on Windows")

        self.getaddr()
        if self.addr is None:
            raise ex.excError('no usable ip to send files to')

        timeout = 5
        cmd = [ 'scp', '-o', 'StrictHostKeyChecking=no',
                       '-o', 'ConnectTimeout='+str(timeout),
                       '-i', self.keyfile(),
                        self.addr+':'+src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        if self.guestos == "windows":
            """ Windows has no sshd.
            """
            raise ex.excNotSupported("remote copy not supported on Windows")

        self.getaddr()
        if self.addr is None:
            raise ex.excError('no usable ip to send files to')

        timeout = 5
        cmd = [ 'scp', '-o', 'StrictHostKeyChecking=no',
                       '-o', 'ConnectTimeout='+str(timeout),
                       '-i', self.keyfile(),
                        src, self.addr+':'+dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcmd(self, cmd):
        if self.guestos == "windows":
            """ Windows has no sshd.
            """
            raise ex.excNotSupported("remote commands not supported on Windows")

        self.getaddr()
        if self.addr is None:
            raise ex.excError('no usable ip to send command to')

        if type(cmd) == str:
            cmd = cmd.split(" ")

        timeout = 5
        cmd = [ 'ssh', '-o', 'StrictHostKeyChecking=no',
                       '-o', 'ForwardX11=no',
                       '-o', 'BatchMode=yes',
                       '-n',
                       '-o', 'ConnectTimeout='+str(timeout),
                       '-i', self.keyfile(),
                        self.addr] + cmd
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def get_subnet(self):
        for subnet in self.cloud.driver.ex_list_subnets():
            if subnet.name == self.subnet_name:
                return subnet
        raise ex.excError("%s subnet not found"%self.subnet_name)

    def get_size(self):
        for size in self.cloud.driver.list_sizes():
            if size.id == self.size_id:
                return size
        raise ex.excError("%s size not found"%self.size_id)

    @lazy
    def cloud(self):
        c = self.svc.node.cloud_get(self.cloud_id)
        return c

    def get_node(self):
        l = self.cloud.list_nodes()
        for n in l:
            if n.name == self.name:
                return n
        return

    def get_image(self, image_id):
        l = self.cloud.driver.list_images(ex_image_ids=[image_id])
        d = {}
        for image in l:
            if image.id == image_id:
                # exact match
                return image
        raise ex.excError("image %s not found" % image_id)

    def has_image(self, image_id):
        l = self.cloud.driver.list_images([image_id])
        for image in l:
            if image.id == image_id:
                return True
        return False

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def getaddr(self):
        if self.addr is not None:
            return
        n = self.get_node()
        if n is None:
            raise ex.excError("could not get node details")
        ips = set(n.public_ips+n.private_ips)
        if len(ips) == 0:
            return 0

        # find first pinging ip
        for ip in ips:
            if check_ping(ip, timeout=1, count=1):
                self.addr = ip
                break

        return 0

    def check_capabilities(self):
        return True

    def ping(self):
        if self.addr is None:
            return 0
        return check_ping(self.addr, timeout=1, count=1)

    def start(self):
        if self.is_up():
            self.log.info("container %s already started" % self.name)
            return
        if rcEnv.nodename in self.svc.drpnodes:
            self.install_drp_flag()
        self.container_start()
        self.can_rollback = True
        self.wait_for_startup()

    def container_start(self):
        """
	    RUNNING = 0
	    REBOOTING = 1
	    TERMINATED = 2
	    PENDING = 3
	    UNKNOWN = 4
	    STOPPED = 5
	    SUSPENDED = 6
	    ERROR = 7
	    PAUSED = 8
        """
        try:
            from libcloud.compute.types import NodeState
        except ImportError:
            raise ex.excError("missing required module libcloud.compute.types")
        n = self.get_node()
        if n is None:
            self.provision()
            return
        elif n.state == NodeState().RUNNING:
            self.log.info("already running")
            return
        elif n.state == NodeState().PENDING:
            self.log.info("already pending. wait for running state.")
            self.wait_for_fn(self.is_up, self.start_timeout, 5)
            return
        elif n.state == NodeState().REBOOTING:
            self.log.info("currently rebooting. wait for running state.")
            self.wait_for_fn(self.is_up, self.start_timeout, 5)
            return
        elif n.state == NodeState().STOPPED:
            self.log.info("starting ebs ec2 instance through aws")
            self.cloud.driver.ex_start_node(n)
            self.log.info("wait for container up status")
            self.wait_for_fn(self.is_up, self.start_timeout, 5)
            return
        raise ex.excError("don't know what to do with node in state: %s"%NodeState().tostring(n.state))

    def container_reboot(self):
        n = self.get_node()
        try:
            self.cloud.driver.reboot_node(n)
        except Exception as e:
            raise ex.excError(str(e))

    def wait_for_startup(self):
        pass

    def stop(self):
        if self.is_down():
            self.log.info("container %s already stopped" % self.name)
            return
        try:
            self.container_stop()
            self.wait_for_shutdown()
        except ex.excError:
            self.container_forcestop()
            self.wait_for_shutdown()

    def container_stop(self):
        cmd = "shutdown -h now"
        self.log.info("remote command: %s"%cmd)
        self.rcmd(cmd)

    def container_forcestop(self):
        n = self.get_node()
        self.log.info("stopping ebs ec2 instance through aws")
        self.cloud.driver.ex_stop_node(n)

    def print_obj(self, n):
        for k in dir(n):
            if '__' in k:
                continue
            print(k, "=", getattr(n, k))

    def is_up(self):
        try:
            from libcloud.compute.types import NodeState
        except ImportError:
            raise ex.excError("missing required module libcloud.compute.types")
        n = self.get_node()
        if n is not None and n.state == NodeState().RUNNING:
            return True
        if n is None:
            self.status_log("state:unknown")
        else:
            self.status_log("state:"+NodeState().tostring(n.state))
        return False

    def get_container_info(self):
        self.info = {'vcpus': '0', 'vmem': '0'}
        n = self.get_node()
        try:
            size = self.cloud.driver.ex_get_size(n.extra['flavorId'])
            self.info['vmem'] = str(size.ram)
        except:
            pass
        return self.info

    def check_manual_boot(self):
        return True

    def install_drp_flag(self):
        pass


