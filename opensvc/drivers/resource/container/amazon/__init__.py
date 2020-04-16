import os

import core.exceptions as ex
import utilities.ping

from .. import \
    BaseContainer, \
    KW_START_TIMEOUT, \
    KW_STOP_TIMEOUT, \
    KW_NO_PREEMPT_ABORT, \
    KW_NAME, \
    KW_HOSTNAME, \
    KW_OSVC_ROOT_PATH, \
    KW_GUESTOS, \
    KW_SHARED_IP_GROUP, \
    KW_SIZE, \
    KW_KEY_NAME, \
    KW_CLOUD_ID, \
    KW_PROMOTE_RW, \
    KW_SCSIRESERV
from core.resource import Resource
from env import Env
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.lazy import lazy
from utilities.proc import justcall


DRIVER_GROUP = "container"
DRIVER_BASENAME = "amazon"
KEYWORDS = [
    KW_START_TIMEOUT,
    KW_STOP_TIMEOUT,
    KW_NO_PREEMPT_ABORT,
    KW_NAME,
    KW_HOSTNAME,
    KW_OSVC_ROOT_PATH,
    KW_GUESTOS,
    KW_SHARED_IP_GROUP,
    KW_SIZE,
    KW_KEY_NAME,
    KW_CLOUD_ID,
    KW_PROMOTE_RW,
    KW_SCSIRESERV,
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    try:
        from libcloud.compute.providers import get_driver
        from libcloud.compute.types import NodeState
        return ["container.amazon"]
    except ImportError:
        return []


class ContainerAmazon(BaseContainer):
    save_timeout = 240

    def __init__(self,
                 cloud_id=None,
                 image_id=None,
                 size="t2.micro",
                 key_name=None,
                 subnet=None,
                 **kwargs):
        super(ContainerAmazon, self).__init__(type="container.amazon", **kwargs)
        self.cloud_id = cloud_id
        self.size_id = size
        self.image_id = image_id
        self.key_name = key_name
        self.subnet_name = subnet
        self.addr = None

    @lazy
    def save_name(self):
        return "%s.save" % self.name

    def keyfile(self):
        kf = [os.path.join(Env.paths.pathetc, self.key_name+'.pem'),
              os.path.join(Env.paths.pathetc, self.key_name+'.pub'),
              os.path.join(Env.paths.pathvar, self.key_name+'.pem'),
              os.path.join(Env.paths.pathvar, self.key_name+'.pub')]
        for k in kf:
            if os.path.exists(k):
                return k
        raise ex.Error("key file for key name '%s' not found"%self.key_name)

    def rcp_from(self, src, dst):
        if self.guestos == "windows":
            """ Windows has no sshd.
            """
            raise ex.NotSupported("remote copy not supported on Windows")

        self.getaddr()
        if self.addr is None:
            raise ex.Error('no usable ip to send files to')

        timeout = 5
        cmd = [ 'scp', '-o', 'StrictHostKeyChecking=no',
                       '-o', 'ConnectTimeout='+str(timeout),
                       '-i', self.keyfile(),
                        self.addr+':'+src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        if self.guestos == "windows":
            """ Windows has no sshd.
            """
            raise ex.NotSupported("remote copy not supported on Windows")

        self.getaddr()
        if self.addr is None:
            raise ex.Error('no usable ip to send files to')

        timeout = 5
        cmd = [ 'scp', '-o', 'StrictHostKeyChecking=no',
                       '-o', 'ConnectTimeout='+str(timeout),
                       '-i', self.keyfile(),
                        src, self.addr+':'+dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcmd(self, cmd):
        if self.guestos == "windows":
            """ Windows has no sshd.
            """
            raise ex.NotSupported("remote commands not supported on Windows")

        self.getaddr()
        if self.addr is None:
            raise ex.Error('no usable ip to send command to')

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
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def get_subnet(self):
        for subnet in self.cloud.driver.ex_list_subnets():
            if subnet.name == self.subnet_name:
                return subnet
        raise ex.Error("%s subnet not found"%self.subnet_name)

    def get_size(self):
        for size in self.cloud.driver.list_sizes():
            if size.id == self.size_id:
                return size
        raise ex.Error("%s size not found"%self.size_id)

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
        raise ex.Error("image %s not found" % image_id)

    def has_image(self, image_id):
        l = self.cloud.driver.list_images([image_id])
        for image in l:
            if image.id == image_id:
                return True
        return False

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

    def getaddr(self):
        if self.addr is not None:
            return
        n = self.get_node()
        if n is None:
            raise ex.Error("could not get node details")
        ips = set(n.public_ips+n.private_ips)
        if len(ips) == 0:
            return 0

        # find first pinging ip
        for ip in ips:
            if utilities.ping.check_ping(ip, timeout=1, count=1):
                self.addr = ip
                break

        return 0

    def check_capabilities(self):
        return True

    def ping(self):
        if self.addr is None:
            return 0
        return utilities.ping.check_ping(self.addr, timeout=1, count=1)

    def start(self):
        if self.is_up():
            self.log.info("container %s already started" % self.name)
            return
        if Env.nodename in self.svc.drpnodes:
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
            raise ex.Error("missing required module libcloud.compute.types")
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
        raise ex.Error("don't know what to do with node in state: %s"%NodeState().tostring(n.state))

    def container_reboot(self):
        n = self.get_node()
        try:
            self.cloud.driver.reboot_node(n)
        except Exception as e:
            raise ex.Error(str(e))

    def wait_for_startup(self):
        pass

    def stop(self):
        if self.is_down():
            self.log.info("container %s already stopped" % self.name)
            return
        try:
            self.container_stop()
            self.wait_for_shutdown()
        except ex.Error:
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
            raise ex.Error("missing required module libcloud.compute.types")
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


    def provisioner(self):
        prereq = True
        if self.image_id is None:
            self.log.error("the image keyword is mandatory for the provision action")
            prereq &= False
        if self.size_id is None:
            self.log.error("the size keyword is mandatory for the provision action")
            prereq &= False
        if self.subnet_name is None:
            self.log.error("the subnet keyword is mandatory for the provision action")
            prereq &= False
        if self.key_name is None:
            self.log.error("the key_name keyword is mandatory for the provision action")
            prereq &= False
        if not prereq:
            raise ex.Error()

        image = self.get_image(self.image_id)
        size = self.get_size()
        subnet = self.get_subnet()
        self.log.info("create instance %s, size %s, image %s, key %s, subnet %s"%(self.name, size.name, image.name, self.key_name, subnet.name))
        self.cloud.driver.create_node(name=self.name, size=size, image=image, ex_keyname=self.key_name, ex_subnet=subnet)
        self.log.info("wait for container up status")
        self.wait_for_fn(self.is_up, self.start_timeout, 5)

