from __future__ import print_function

import os

import core.exceptions as ex
import utilities.ping


from .. import \
    BaseContainer, \
    KW_SNAP, \
    KW_SNAPOF, \
    KW_VIRTINST, \
    KW_START_TIMEOUT, \
    KW_STOP_TIMEOUT, \
    KW_NO_PREEMPT_ABORT, \
    KW_NAME, \
    KW_HOSTNAME, \
    KW_OSVC_ROOT_PATH, \
    KW_GUESTOS, \
    KW_PROMOTE_RW, \
    KW_SCSIRESERV, \
    KW_QGA
from core.resource import Resource
from env import Env
from utilities.cache import cache, clear_cache
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.proc import justcall, which

CAPABILITIES = {
    "partitions": "1.0.1",
}

DRIVER_GROUP = "container"
DRIVER_BASENAME = "kvm"
KEYWORDS = [
    KW_SNAP,
    KW_SNAPOF,
    KW_VIRTINST,
    KW_START_TIMEOUT,
    KW_STOP_TIMEOUT,
    KW_NO_PREEMPT_ABORT,
    KW_NAME,
    KW_HOSTNAME,
    KW_OSVC_ROOT_PATH,
    KW_GUESTOS,
    KW_PROMOTE_RW,
    KW_SCSIRESERV,
    KW_QGA,
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    data = []
    cmd = ['virsh', 'capabilities']
    out, err, ret = justcall(cmd)
    if ret == 0:
        data.append("container.kvm")
    return data


class ContainerKvm(BaseContainer):
    def __init__(self,
                 snap=None,
                 snapof=None,
                 virtinst=None,
                 qga=False,
                 **kwargs):
        super(ContainerKvm, self).__init__(type="container.kvm", **kwargs)
        self.refresh_provisioned_on_provision = True
        self.refresh_provisioned_on_unprovision = True
        self.snap = snap
        self.snapof = snapof
        self.virtinst = virtinst or []
        self.qga = qga

    @lazy
    def cf(self):
        return os.path.join(os.sep, 'etc', 'libvirt', 'qemu', self.name+'.xml')

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

    def list_kvmconffiles(self):
        if not self.shared and not self.svc.topology == "failover":
            # don't send the container cf to nodes that won't run it
            return []
        if os.path.exists(self.cf):
            return [self.cf] + self.firmware_files()
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
        if self.qga:
            return
        return utilities.ping.check_ping(self.addr, timeout=1, count=1)

    def qga_exec_status(self, pid):
        import json
        import base64
        from utilities.string import bdecode
        payload = {
            "execute": "guest-exec-status",
            "arguments": {
                "pid": pid
            }
        }
        cmd = ["virsh", "qemu-agent-command", self.name, json.dumps(payload)]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        data = json.loads(out)["return"]
        data["out-data"] = bdecode(base64.b64decode(data.get("out-data", b"")))
        data["err-data"] = bdecode(base64.b64decode(data.get("err-data", b"")))
        return data

    def qga_operational(self):
        import json
        payload = {
            "execute": "guest-exec",
            "arguments": {
                "path": "/usr/bin/pwd",
                "arg": [],
                "capture-output": True
            }
        }
        cmd = ["virsh", "qemu-agent-command", self.name, json.dumps(payload)]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.log.debug(err)
            return False
        return True

    def rcp_from(self, src, dst):
        if self.qga:
            # TODO
            return "", "", 0
        else:
            cmd = Env.rcp.split() + [self.name+":"+src, dst]
            return justcall(cmd)

    def rcp(self, src, dst):
        if self.qga:
            return self.qga_cp(src, dst)
        else:
            cmd = Env.rcp.split() + [src, self.name+':'+dst]
            return justcall(cmd)

    def qga_cp(self, src, dst):
        self.log.debug("qga cp: %s to %s", src, dst)
        import base64
        import json
        import time
        payload = {
            "execute":"guest-file-open",
            "arguments":{
                "path": dst,
                "mode":"w"
            }
        }
        cmd = ["virsh", "qemu-agent-command", self.name, json.dumps(payload)]
        out, err, ret = justcall(cmd)
        self.log.debug("%s => out:%s err:%s ret:%d", payload, out, err, ret)
        if ret != 0:
            raise ex.Error(err)
        data = json.loads(out)
        handle = data["return"]

        with open(src, "rb") as f:
            buff = base64.b64encode(f.read()).decode()

        payload = {
            "execute":"guest-file-write",
            "arguments":{
                "handle": handle,
                "buf-b64": buff,
            }
        }
        cmd = ["virsh", "qemu-agent-command", self.name, json.dumps(payload)]
        out, err, ret = justcall(cmd)
        self.log.debug("%s => out:%s err:%s ret:%d", payload, out, err, ret)
        if ret != 0:
            raise ex.Error(err)
        data = json.loads(out)

        payload = {
            "execute":"guest-file-close",
            "arguments":{
                "handle": handle,
            }
        }
        cmd = ["virsh", "qemu-agent-command", self.name, json.dumps(payload)]
        out, err, ret = justcall(cmd)
        self.log.debug("%s => out:%s err:%s ret:%d", payload, out, err, ret)
        if ret != 0:
            raise ex.Error(err)
        return "", "", 0

    def qga_exec(self, cmd, verbose=False, timeout=60):
        if verbose:
            log = self.log.info
        else:
            log = self.log.debug
        log("qga exec: %s", " ".join(cmd))
        import json
        import time
        payload = {
            "execute": "guest-exec",
            "arguments": {
                "path": cmd[0],
                "arg": cmd[1:],
                "capture-output": True
            }
        }
        cmd = ["virsh", "qemu-agent-command", self.name, json.dumps(payload)]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.log.debug(err)
            return False
        data = json.loads(out)
        pid = data["return"]["pid"]
        log("qga exec: command started with pid %d", pid)
        for i in range(timeout):
            data = self.qga_exec_status(pid)
            if not data.get("exited"):
                time.sleep(1)
                continue
            log("qga exec: command exited with %d", data.get("exitcode"))
            #log("qga exec: out: %s", data.get("out-data"))
            #log("qga exec: err: %s", data.get("err-data"))
            return data
        raise ex.Error("timeout waiting for qemu guest exec result, pid %d" % pid)

    def rcmd(self, cmd):
        if self.qga:
            data = self.qga_exec(cmd)
            return data.get("out-data", ""), data.get("err-data", ""), data.get("exitcode", 1)
        elif hasattr(self, "runmethod"):
            cmd = self.runmethod + cmd
            return justcall(cmd, stdin=self.svc.node.devnull)
        else:
            raise ex.EncapUnjoinable("undefined rcmd/runmethod in resource %s" % self.rid)

    def operational(self):
        if self.qga:
            return self.qga_operational()
        else:
            return BaseContainer.operational(self)

    def is_up_clear_cache(self):
        clear_cache("virsh.dom_state.%s@%s" % (self.name, Env.nodename))

    def virsh_define(self):
        cmd = ['virsh', 'define', self.cf]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def virsh_undefine(self):
        if self.has_efi():
            cmd = ['virsh', 'undefine', '--nvram', self.name]
        else:
            cmd = ['virsh', 'undefine', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_start(self):
        if self.svc.create_pg and which("machinectl") is None and self.capable("partitions"):
            self.set_partition()
        else:
            self.unset_partition()
        if not os.path.exists(self.cf):
            self.log.error("%s not found"%self.cf)
            raise ex.Error
        self.virsh_define()
        cmd = ['virsh', 'start', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        clear_cache("virsh.dom_state.%s@%s" % (self.name, Env.nodename))

    def start(self):
        super(ContainerKvm, self).start()

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
            raise ex.Error
        clear_cache("virsh.dom_state.%s@%s" % (self.name, Env.nodename))

    def stop(self):
        super(ContainerKvm, self).stop()

    def container_forcestop(self):
        cmd = ['virsh', 'destroy', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

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
            cmd = Env.rsh.split() + [nodename] + cmd
        return self._dom_state(self.name, nodename if nodename else Env.nodename, cmd)

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

    def is_defined(self):
        if os.path.exists(self.cf):
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
        from xml.etree.ElementTree import ElementTree
        tree = ElementTree()
        try:
            tree.parse(self.cf)
        except Exception as exc:
            raise ex.Error("container config parsing error: %s" % exc)
        root = tree.getroot()
        if root is None:
            raise ex.Error("invalid container config %s" % self.cf)
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
            raise ex.Error("container config parsing error: %s" % exc)
        root = tree.getroot()
        if root is None:
            raise ex.Error("invalid container config %s" % self.cf)
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
        flag_disk_path = os.path.join(Env.paths.pathvar, 'drp_flag.vdisk')

        from xml.etree.ElementTree import ElementTree, SubElement
        tree = ElementTree()
        try:
            tree.parse(self.cf)
        except Exception as exc:
            raise ex.Error("container config parsing error: %s" % exc)

        # create the vdisk if it does not exist yet
        if not os.path.exists(flag_disk_path):
            with open(flag_disk_path, 'w') as f:
                f.write('')
                f.close()

        # check if drp flag is already set up
        for disk in tree.iter("disk"):
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

    def firmware_files(self):
        l = []
        from xml.etree.ElementTree import ElementTree
        tree = ElementTree()
        try:
            tree.parse(self.cf)
        except Exception as exc:
            return l
        for xml_node in tree.findall("os"):
            s = xml_node.find("loader")
            if s is not None:
                l.append(s.text)
            s = xml_node.find("nvram")
            if s is not None:
                l.append(s.text)
        return l

    def has_efi(self):
        from xml.etree.ElementTree import ElementTree
        tree = ElementTree()
        try:
            tree.parse(self.cf)
        except Exception as exc:
            return False
        for xml_node in tree.findall("os"):
            if xml_node.attrib.get("firmware") == "efi":
                return True
            if xml_node.find("nvram") is not None:
                return True
        return False

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

        from xml.etree.ElementTree import ElementTree
        tree = ElementTree()
        try:
            tree.parse(self.cf)
        except Exception as exc:
            return data
        for dev in tree.iter('disk'):
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
        return super(ContainerKvm, self)._status(verbose=verbose)

    def check_kvm(self):
        if os.path.exists(self.cf):
            return True
        return False

    def setup_kvm(self):
        if self.virtinst is None:
            self.log.error("the 'virtinst' parameter must be set")
            raise ex.Error
        cmd = [] + self.virtinst
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def setup_ips(self):
        if self.qga:
            return
        self.purge_known_hosts()
        for resource in self.svc.get_resources("ip"):
            self.purge_known_hosts(resource.addr)

    def purge_known_hosts(self, ip=None):
        if ip is None:
            cmd = ['ssh-keygen', '-R', self.svc.name]
        else:
            cmd = ['ssh-keygen', '-R', ip]
        ret, out, err = self.vcall(cmd, err_to_info=True)

    def setup_snap(self):
        if self.snap is None and self.snapof is None:
            return
        elif self.snap and self.snapof is None:
            self.log.error("the 'snapof' parameter is required when 'snap' parameter present")
            raise ex.Error
        elif self.snapof and self.snap is None:
            self.log.error("the 'snap' parameter is required when 'snapof' parameter present")
            raise ex.Error

        if not which('btrfs'):
            self.log.error("'btrfs' command not found")
            raise ex.Error

        cmd = ['btrfs', 'subvolume', 'snapshot', self.snapof, self.snap]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def provisioner(self):
        self.setup_snap()
        self.setup_kvm()
        self.setup_ips()
        self.log.info("provisioned")
        return True

    def provisioned(self):
        cmd = ['virsh', 'dominfo', self.name]
        out, _, ret = justcall(cmd)
        if ret != 0:
            return False
        return True

    def unprovisioner(self):
        if not self.provisioned():
            self.log.debug("skip kvm unprovision: container is not provisioned")
            return
        if self.is_defined():
            self.virsh_undefine()
        self.log.info("unprovisioned")
        return True

    def unprovisioner_shared_non_leader(self):
        if not self.provisioned():
            self.log.debug("skip kvm unprovision: container is not provisioned")
            return
        if self.is_defined():
            self.virsh_undefine()
        self.log.info("unprovisioned")
        return True
