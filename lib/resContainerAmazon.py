#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import rcStatus
import resources as Res
import time
import os
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import justcall
from rcUtilitiesLinux import check_ping
import resContainer

class CloudVm(resContainer.Container):
    startup_timeout = 240
    shutdown_timeout = 120
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
                 optional=False,
                 disabled=False,
                 monitor=False,
                 restart=0,
                 subset=None,
                 tags=set([]),
                 always_on=set([])):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.amazon",
                                        guestos=guestos,
                                        optional=optional,
                                        disabled=disabled,
                                        monitor=monitor,
                                        restart=restart,
                                        subset=subset,
                                        tags=tags,
                                        always_on=always_on)
        self.cloud_id = cloud_id
        self.save_name = name + '.save'
        self.size_id = size
        self.image_id = image_id
        self.key_name = key_name
        self.subnet_name = subnet
        self.addr = None

    def keyfile(self):
        kf = [os.path.join(rcEnv.pathetc, self.key_name+'.pem'),
              os.path.join(rcEnv.pathetc, self.key_name+'.pub'),
              os.path.join(rcEnv.pathvar, self.key_name+'.pem'),
              os.path.join(rcEnv.pathvar, self.key_name+'.pub')]
        for k in kf:
            if os.path.exists(k):
                return k
        raise ex.excError("key file for key name '%s' not found"%self.key_name)

    def rcp_from(self, src, dst):
        if self.guestos == "Windows":
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
        if self.guestos == "Windows":
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
        if self.guestos == "Windows":
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
        c = self.get_cloud()
        for subnet in c.driver.ex_list_subnets():
            if subnet.name == self.subnet_name:
                return subnet
        raise ex.excError("%s subnet not found"%self.subnet_name)

    def get_size(self):
        c = self.get_cloud()
        for size in c.driver.list_sizes():
            if size.id == self.size_id:
                return size
        raise ex.excError("%s size not found"%self.size_name)

    def get_cloud(self):
        if hasattr(self, 'cloud'):
            return self.cloud
        c = self.svc.node.cloud_get(self.cloud_id)
        self.cloud = c
        return self.cloud

    def get_node(self):
        c = self.get_cloud()
        l = c.list_nodes()
        for n in l:
            if n.name == self.name:
                return n
        return

    def get_image(self, image_id):
        c = self.get_cloud()
        l = c.driver.list_images(ex_image_ids=[image_id])
        d = {}
        for image in l:
            if image.id == image_id:
                # exact match
                return image
        raise ex.excError("image %s not found" % image_id)

    def has_image(self, image_id):
        c = self.get_cloud()
        l = c.driver.list_images([image_id])
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

    def files_to_sync(self):
        return []

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
        from libcloud.compute.types import NodeState
        n = self.get_node()
        if n is None:
            self.provision()
            return
        elif n.state == NodeState().RUNNING:
            self.log.info("already running")
            return
        elif n.state == NodeState().PENDING:
            self.log.info("already pending. wait for running state.")
            self.wait_for_fn(self.is_up, self.startup_timeout, 5)
            return
        elif n.state == NodeState().REBOOTING:
            self.log.info("currently rebooting. wait for running state.")
            self.wait_for_fn(self.is_up, self.startup_timeout, 5)
            return
        elif n.state == NodeState().STOPPED:
            c = self.get_cloud()
            self.log.info("starting ebs ec2 instance through aws")
            c.driver.ex_start_node(n)
            self.log.info("wait for container up status")
            self.wait_for_fn(self.is_up, self.startup_timeout, 5)
            return
        raise ex.excError("don't know what to do with node in state: %s"%NodeState().tostring(n.state))

    def container_reboot(self):
        c = self.get_cloud()
        n = self.get_node()
        try:
            c.driver.reboot_node(n)
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
        c = self.get_cloud()
        n = self.get_node()
        self.log.info("stopping ebs ec2 instance through aws")
        c.driver.ex_stop_node(n)

    def print_obj(self, n):
        for k in dir(n):
            if '__' in k:
                continue
            print(k, "=", getattr(n, k))

    def is_up(self):
        from libcloud.compute.types import NodeState
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
        c = self.get_cloud()
        n = self.get_node()
        try:
            size = c.driver.ex_get_size(n.extra['flavorId'])
            self.info['vmem'] = str(size.ram)
        except:
            pass
        return self.info

    def check_manual_boot(self):
        return True

    def install_drp_flag(self):
        pass

    def provision(self):
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
            raise ex.excError()

        c = self.get_cloud()
        image = self.get_image(self.image_id)
        size = self.get_size()
        subnet = self.get_subnet()
        self.log.info("create instance %s, size %s, image %s, key %s, subnet %s"%(self.name, size.name, image.name, self.key_name, subnet.name))
        c.driver.create_node(name=self.name, size=size, image=image, ex_keyname=self.key_name, ex_subnet=subnet)
        self.log.info("wait for container up status")
        self.wait_for_fn(self.is_up, self.startup_timeout, 5)

