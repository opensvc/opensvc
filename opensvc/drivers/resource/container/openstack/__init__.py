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
from env import Env
from utilities.lazy import lazy
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.lazy import lazy
from utilities.proc import justcall

DRIVER_GROUP = "container"
DRIVER_BASENAME = "openstack"
KEYWORDS = [
    {
        "keyword": "template",
        "text": "The name of the openstack template image to derive from.",
        "required": True,
        "provisioning": True
    },
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
        from libcloud.compute.types import Provider
        from libcloud.compute.providers import get_driver
        import libcloud.security
        return ["container.openstack"]
    except ImportError:
        return []

class ContainerOpenstack(BaseContainer):
    save_timeout = 240

    def __init__(self,
                 cloud_id=None,
                 size="tiny",
                 key_name=None,
                 shared_ip_group=None,
                 template=None,
                 **kwargs):
        super(ContainerOpenstack, self).__init__(type="container.openstack", **kwargs)
        self.cloud_id = cloud_id
        self.size_name = size
        self.key_name = key_name
        self.shared_ip_group = shared_ip_group
        self.template = template
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
            raise ex.Error('no usable public ip to send files to')

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
            raise ex.Error('no usable public ip to send files to')

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
            raise ex.Error('no usable public ip to send command to')

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

    def get_size(self):
        for size in self.cloud.driver.list_sizes():
            if size.name == self.size_name:
                return size
        raise ex.Error("%s size not found"%self.size_name)

    @lazy
    def cloud(self):
        return self.svc.node.cloud_get(self.cloud_id)

    def get_node(self):
        l = self.cloud.list_nodes()
        for n in l:
            if n.name == self.name:
                return n
        return

    def get_save_name(self):
        import datetime
        now = datetime.datetime.now()
        save_name = self.save_name + now.strftime(".%Y-%m-%d.%H:%M:%S")
        return save_name

    def purge_saves(self):
        l = self.cloud.driver.list_images()
        d = {}
        for image in l:
            if image.name.startswith(self.save_name):
                d[image.name] = image
        if len(d) == 0:
             raise ex.Error("no save image found")
        elif len(d) == 1:
             self.log.info("no previous save image to delete")
        for k in sorted(d.keys())[:-1]:
             self.log.info("delete previous save image %s"%d[k].name)
             self.cloud.driver.ex_delete_image(d[k])

    def get_last_save(self):
        return self.get_image(self.save_name)

    def get_template(self):
        return self.get_image(self.template)

    def get_image(self, name):
        l = self.cloud.driver.list_images()
        d = {}
        for image in l:
            if image.name == name:
                # exact match
                return image
            elif image.name.startswith(name):
                d[image.name] = image
        if len(d) == 0:
             raise ex.Error("image %s not found"%name)
        for k in sorted(d.keys()):
             last = d[k]
        return last

    def has_image(self, name):
        l = self.cloud.driver.list_images()
        for image in l:
            if image.name == name:
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
        n = self.get_node()
        if n is not None:
            if n.state == 4:
                self.log.info("reboot %s"%self.name)
                self.container_reboot()
            else:
                raise ex.Error("abort reboot because vm is in state %d (!=4)"%n.state)
        else:
            self.container_restore()

    def container_reboot(self):
        n = self.get_node()
        try:
            self.cloud.driver.reboot_node(n)
        except Exception as e:
            raise ex.Error(str(e))

    def container_restore(self):
        image = self.get_last_save()
        size = self.get_size()
        self.log.info("create instance %s, size %s, image %s, key %s"%(self.name, size.name, image.name, self.key_name))
        n = self.cloud.driver.create_node(name=self.name, size=size, image=image, ex_keyname=self.key_name, ex_shared_ip_group_id=self.shared_ip_group)
        self.log.info("wait for container up status")
        self.wait_for_fn(self.is_up, self.start_timeout, 5)
        #n = self.cloud.driver.ex_update_node(n, accessIPv4='46.231.128.84')

    def wait_for_startup(self):
        pass

    def stop(self):
        if self.is_down():
            self.log.info("container %s already stopped" % self.name)
            return
        self.container_stop()
        try:
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
        self.container_save()
        self.cloud.driver.destroy_node(n)
        self.purge_saves()

    def print_obj(self, n):
        for k in dir(n):
            if '__' in k:
                continue
            print(k, "=", getattr(n, k))

    def container_save(self):
        n = self.get_node()
        save_name = self.get_save_name()
        if self.has_image(save_name):
            return
        #self.print_obj(n)
        if n.state == 9999:
            self.log.info("a save is already in progress")
            return
        self.log.info("save new image %s"%save_name)
        try:
            image = self.cloud.driver.ex_save_image(n, save_name)
        except Exception as e:
            raise ex.Error(str(e))
        import time
        delay = 5
        for i in range(self.save_timeout//delay):
            img = self.cloud.driver.ex_get_image(image.id)
            if img.extra['status'] != 'SAVING':
                break
            time.sleep(delay)
        if img.extra['status'] != 'ACTIVE':
            raise ex.Error("save failed, image status %s"%img.extra['status'])

    def is_up(self):
        n = self.get_node()
        if n is not None and n.state == 0:
            return True
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

    def provision(self):
        image = self.get_template()
        size = self.get_size()
        self.log.info("create instance %s, size %s, image %s, key %s"%(self.name, size.name, image.name, self.key_name))
        self.cloud.driver.create_node(name=self.name, size=size, image=image, ex_keyname=self.key_name, ex_shared_ip_group_id=self.shared_ip_group)
        #self.wait_for_startup()
        self.wait_for_fn(self.is_up, self.start_timeout, 5)

