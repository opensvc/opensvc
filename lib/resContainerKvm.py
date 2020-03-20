import os
import time

import rcExceptions as ex
import rcStatus
import resContainer
import resources as Res

from rcGlobalEnv import rcEnv
from rcUtilities import justcall, cache, clear_cache, lazy, which
from rcUtilitiesLinux import check_ping
from svcBuilder import init_kwargs, container_kwargs
from svcdict import KEYS

CAPABILITIES = {
    "partitions": "1.0.1",
}

DRIVER_GROUP = "container"
DRIVER_BASENAME = "kvm"
KEYWORDS = [
    resContainer.KW_SNAP,
    resContainer.KW_SNAPOF,
    resContainer.KW_VIRTINST,
    resContainer.KW_START_TIMEOUT,
    resContainer.KW_STOP_TIMEOUT,
    resContainer.KW_NO_PREEMPT_ABORT,
    resContainer.KW_NAME,
    resContainer.KW_HOSTNAME,
    resContainer.KW_OSVC_ROOT_PATH,
    resContainer.KW_GUESTOS,
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
    r = Kvm(**kwargs)
    svc += r


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

    def capable(self, cap):
        if self.libvirt_version >= CAPABILITIES.get(cap, "0"):
            return True
        return False

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

    def is_up_clear_cache(self):
        clear_cache("virsh.dom_state.%s@%s" % (self.name, rcEnv.nodename))

    def virsh_define(self):
        cmd = ['virsh', 'define', self.cf]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_start(self):
        if self.svc.create_pg and which("machinectl") is None and self.capable("partitions"):
            self.set_partition()
        else:
            self.unset_partition()
        if not os.path.exists(self.cf):
            self.log.error("%s not found"%self.cf)
            raise ex.excError
        self.virsh_define()
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

    @lazy
    def cgroup_dir(self):
        return "/"+self.svc.pg.get_cgroup_relpath(self)

    @lazy
    def libvirt_version(self):
        cmd = ["virsh", "--version"]
        out, _, _ = justcall(cmd)
        return out.strip()

    def unset_partition(self):
        from xml.etree.ElementTree import ElementTree, SubElement
        tree = ElementTree()
        try:
            tree.parse(self.cf)
        except Exception as exc:
            raise ex.excError("container config parsing error: %s" % exc)
        root = tree.getroot()
        if root is None:
            raise ex.excError("invalid container config %s" % self.cf)
        resource = root.find("resource")
        if resource is None:
            return
        part = resource.find("partition")
        if part is None:
            return
        if part.text != self.cgroup_dir:
            return
        root.remove(resource)
        self.log.info("unset resource/partition = %s" % self.cgroup_dir)
        part.text = self.cgroup_dir
        tree.write(self.cf)

    def set_partition(self):
        self.svc.pg.create_pg(self)
        from xml.etree.ElementTree import ElementTree, SubElement
        tree = ElementTree()
        try:
            tree.parse(self.cf)
        except Exception as exc:
            raise ex.excError("container config parsing error: %s" % exc)
        root = tree.getroot()
        if root is None:
            raise ex.excError("invalid container config %s" % self.cf)
        resource = root.find("resource")
        if resource is None:
            resource = SubElement(root, "resource")
        part = resource.find("partition")
        if part is None:
            print("create part")
            part = SubElement(resource, "partition")
        if part.text == self.cgroup_dir:
            return
        self.log.info("set resource/partition = %s" % self.cgroup_dir)
        part.text = self.cgroup_dir
        tree.write(self.cf)

    def install_drp_flag(self):
        flag_disk_path = os.path.join(rcEnv.paths.pathvar, 'drp_flag.vdisk')

        from xml.etree.ElementTree import ElementTree, SubElement
        tree = ElementTree()
        try:
            tree.parse(self.cf)
        except Exception as exc:
            raise ex.excError("container config parsing error: %s" % exc)

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
        try:
            tree.parse(self.cf)
        except Exception as exc:
            return data
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

