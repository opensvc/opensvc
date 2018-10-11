import rcStatus
import resources as Res
import time
import os
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import justcall, cache, clear_cache, lazy
from rcUtilitiesLinux import check_ping
import resContainer

class Kvm(resContainer.Container):
    def __init__(self,
                 rid,
                 name,
                 guestos=None,
                 osvc_root_path=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.kvm",
                                        guestos=guestos,
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self.cf = os.path.join(os.sep, 'etc', 'libvirt', 'qemu', name+'.xml')

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def list_kvmconffiles(self):
        if not self.shared and not self.svc.topology == "failover":
            # don't send the container cf to nodes that won't run it
            return []
        if os.path.exists(self.cf):
            return [self.cf]
        return []

    def files_to_sync(self):
        return self.list_kvmconffiles()

    @cache("virsh.capabilities")
    def capabilities(self):
        cmd = ['virsh', 'capabilities']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        return out

    def check_capabilities(self):
        out = self.capabilities()
        if out is None:
            self.status_log("can not fetch capabilities")
            return False
        if 'hvm' not in out:
            self.status_log("hvm not supported by host")
            return False
        return True

    def ping(self):
        return check_ping(self.addr, timeout=1, count=1)

    def is_up_clear_caches(self):
        clear_cache("virsh.dom_state.%s@%s" % (self.name, rcEnv.nodename))

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
        clear_cache("virsh.dom_state.%s@%s" % (self.name, rcEnv.nodename))

    def start(self):
        resContainer.Container.start(self)

    def container_stop(self):
        state = self.dom_state()
        if state == "running":
            cmd = ['virsh', 'shutdown', self.name]
        elif state in ("blocked", "paused", "crashed"):
            self.container_forcestop()
        else:
            self.log.info("skip stop, container state=%s", state)
            return
        ret, buff, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        clear_cache("virsh.dom_state.%s@%s" % (self.name, rcEnv.nodename))

    def stop(self):
        resContainer.Container.stop(self)

    def container_forcestop(self):
        cmd = ['virsh', 'destroy', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    @cache("virsh.dom_state.{args[1]}@{args[2]}")
    def _dom_state(self, vmname, nodename, cmd):
        ret, out, err = self.call(cmd, errlog=False)
        if ret != 0:
            return
        for line in out.splitlines():
            if line.startswith("State:"):
                return line.split(":", 1)[-1].strip()

    def dom_state(self, nodename=None):
        cmd = ['virsh', 'dominfo', self.name]
        if nodename is not None:
            cmd = rcEnv.rsh.split() + [nodename] + cmd
        return self._dom_state(self.name, nodename if nodename else rcEnv.nodename, cmd)

    def is_up(self, nodename=None):
        state = self.dom_state(nodename=nodename)
        if state == "running":
            return True
        return False

    def is_down(self, nodename=None):
        state = self.dom_state(nodename=nodename)
        if state in (None, "shut off", "no state"):
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
        flag_disk_path = os.path.join(rcEnv.paths.pathvar, 'drp_flag.vdisk')

        from xml.etree.ElementTree import ElementTree, SubElement
        tree = ElementTree()
        tree.parse(self.cf)

        # create the vdisk if it does not exist yet
        if not os.path.exists(flag_disk_path):
            with open(flag_disk_path, 'w') as f:
                f.write('')
                f.close()

        # check if drp flag is already set up
        for disk in tree.getiterator("disk"):
            e = disk.find('source')
            if e is None:
                continue
            (dev, path) = e.items()[0]
            if path == flag_disk_path:
                self.log.info("flag virtual disk already exists")
                return

        # add vdisk to the vm xml config
        self.log.info("install drp flag virtual disk")
        devices = tree.find("devices")
        e = SubElement(devices, "disk", {'device': 'disk', 'type': 'file'})
        SubElement(e, "driver", {'name': 'qemu'})
        SubElement(e, "source", {'file': flag_disk_path})
        SubElement(e, "target", {'bus': 'virtio', 'dev': 'vdosvc'})
        tree.write(self.cf)

    def sub_devs(self):
        devs = set(map(lambda x: x[0], self.devmapping))
        return devs

    @lazy
    def devmapping(self):
        """
        Return a list of (src, dst) devices tuples fount in the container
        conifguration file.
        """
        if not os.path.exists(self.cf):
            # not yet received from peer node ?
            return []
        data = []

        from xml.etree.ElementTree import ElementTree, SubElement
        tree = ElementTree()
        tree.parse(self.cf)
        for dev in tree.getiterator('disk'):
            s = dev.find('source')
            if s is None:
                 continue
            if 'dev' not in s.attrib:
                 continue
            src = s.attrib['dev']
            s = dev.find('target')
            if s is None:
                 continue
            if 'dev' not in s.attrib:
                 continue
            dst = s.attrib['dev']
            data.append((src, dst))
        return data

    def _status(self, verbose=False):
        return resContainer.Container._status(self, verbose=verbose)

