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
import rcStatus
import resources as Res
import time
import os
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import qcall
from rcUtilitiesLinux import check_ping
import resContainer

class Kvm(resContainer.Container):
    startup_timeout = 180
    shutdown_timeout = 120

    def __init__(self, name, optional=False, disabled=False, monitor=False,
                 tags=set([])):
        resContainer.Container.__init__(self, rid="kvm", name=name,
                                        type="container.kvm",
                                        optional=optional, disabled=disabled,
                                        monitor=monitor, tags=tags)
        self.cf = os.path.join(os.sep, 'etc', 'libvirt', 'qemu', name+'.xml')

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def list_kvmconffiles(self):
        if os.path.exists(self.cf):
            return [self.cf]
        return []

    def files_to_sync(self):
        return self.list_kvmconffiles()

    def check_capabilities(self):
        cmd = ['virsh', 'capabilities']
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            self.status_log("can not fetch capabilities")
            return False
        if 'hvm' not in out:
            self.status_log("hvm not supported by host")
            return False
        return True

    def ping(self):
        return check_ping(self.addr, timeout=1, count=1)

    def container_start(self):
        if not os.path.exists(self.cf):
            self.log.error("%s not found"%self.cf)
            raise ex.excError
        cmd = ['virsh', 'define', self.cf]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        cmd = ['virsh', 'start', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_stop(self):
        cmd = ['virsh', 'shutdown', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_forcestop(self):
        cmd = ['virsh', 'destroy', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def is_up(self):
        cmd = ['virsh', 'dominfo', self.name]
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            return False
        if "running" in out.split():
            return True
        return False

    def get_container_info(self):
        cmd = ['virsh', 'dominfo', self.name]
        (ret, out, err) = self.call(cmd, errlog=False, cache=True)
        self.info = {'vcpus': '0', 'vmem': '0'}
        if ret != 0:
            return self.info
        for line in out.split('\n'):
            if "CPU(s):" in line: self.info['vcpus'] = line.split(':')[1].strip()
            if "Used memory:" in line: self.info['vmem'] = line.split(':')[1].strip()
        return self.info

    def check_manual_boot(self):
        cf = os.path.join(os.sep, 'etc', 'libvirt', 'qemu', 'autostart', self.name+'.xml')
        if os.path.exists(cf):
            return False
        return True

    def install_drp_flag(self):
        flag_disk_path = os.path.join(rcEnv.pathvar, 'drp_flag.vdisk')

        from xml.etree.ElementTree import ElementTree, SubElement
        tree = ElementTree()
        tree.parse(self.cf)

        """ create the vdisk if it does not exist yet
        """
        if not os.path.exists(flag_disk_path):
            with open(flag_disk_path, 'w') as f:
                f.write('')
                f.close()

        """ check if drp flag is already set up
        """
        for disk in tree.getiterator("disk"):
            e = disk.find('source')
            if e is None:
                continue
            (dev, path) = e.items()[0]
            if path == flag_disk_path:
                self.log.info("flag virtual disk already exists")
                return

        """ add vdisk to the vm xml config
        """
        self.log.info("install drp flag virtual disk")
        devices = tree.find("devices")
        e = SubElement(devices, "disk", {'device': 'disk', 'type': 'file'})
        SubElement(e, "driver", {'name': 'qemu'})
        SubElement(e, "source", {'file': flag_disk_path})
        SubElement(e, "target", {'bus': 'virtio', 'dev': 'vdosvc'})
        tree.write(self.cf)

    def provision(self):
        m = __import__("provKvm")
        prov = m.ProvisioningKvm(self)
        prov.provisioner()

