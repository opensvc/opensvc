from __future__ import print_function

import os
import json
import time
import base64

from xml.etree.ElementTree import ElementTree, SubElement

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
    KW_QGA, \
    KW_QGA_OPERATIONAL_DELAY
from core.resource import Resource
from env import Env
from utilities.cache import cache, clear_cache
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.proc import justcall, which
from utilities.string import bdecode

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
    KW_QGA_OPERATIONAL_DELAY,
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
    # The mode qga_cp() sets on the files it copies into the guest. They are
    # staged there for the encap agent, which reads them as root, so nobody
    # else in the guest has any business with them.
    qga_cp_mode = "600"

    def __init__(self,
                 snap=None,
                 snapof=None,
                 virtinst=None,
                 qga=False,
                 qga_operational_delay=10,
                 **kwargs):
        super(ContainerKvm, self).__init__(type="container.kvm", **kwargs)
        self.refresh_provisioned_on_provision = True
        self.refresh_provisioned_on_unprovision = True
        self.snap = snap
        self.snapof = snapof
        self.virtinst = virtinst or []
        self.qga = qga
        self.qga_operational_delay = qga_operational_delay

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

    def qga_agent_command(self, payload):
        cmd = ["virsh", "qemu-agent-command", self.name, json.dumps(payload)]
        out, err, ret = justcall(cmd)
        self.log.debug("%s => out:%s err:%s ret:%d", payload, out, err, ret)
        if ret != 0:
            raise ex.Error(err)
        return json.loads(out)

    def qga_file_open(self, path, mode):
        payload = {
            "execute":"guest-file-open",
            "arguments":{
                "path": path,
                "mode": mode
            }
        }
        return self.qga_agent_command(payload)["return"]

    def qga_file_write(self, handle, buff):
        payload = {
            "execute":"guest-file-write",
            "arguments":{
                "handle": handle,
                "buf-b64": buff,
            }
        }
        self.qga_agent_command(payload)

    def qga_file_close(self, handle):
        payload = {
            "execute":"guest-file-close",
            "arguments":{
                "handle": handle,
            }
        }
        self.qga_agent_command(payload)

    def qga_chmod(self, path, mode):
        data = self.qga_exec(["/bin/chmod", mode, path])
        if not data or data.get("exitcode") != 0:
            self.log.warning("qga cp: could not set mode %s on %s: the file "
                             "stays world-writable, as the guest agent "
                             "created it", mode, path)

    def qga_cp(self, src, dst):
        """
        Copy <src> to <dst> in the guest, through the qemu guest agent.

        The guest agent creates the files it opens world-writable: its
        guest-file-open takes no mode, and the agent chmods every file it
        creates to 0666, its own umask included. What lands in the guest here
        is read back by the encap agent as root, so the destination is locked
        down before anything is written into it: the file is created empty,
        chmoded, then written, and reopening a file that already exists
        leaves its mode alone.
        """
        self.log.debug("qga cp: %s to %s", src, dst)
        self.qga_file_close(self.qga_file_open(dst, "w"))
        self.qga_chmod(dst, self.qga_cp_mode)

        with open(src, "rb") as f:
            buff = base64.b64encode(f.read()).decode()

        handle = self.qga_file_open(dst, "w")
        try:
            self.qga_file_write(handle, buff)
        finally:
            self.qga_file_close(handle)
        return "", "", 0

    def qga_exec(self, cmd, verbose=False, timeout=60):
        if verbose:
            log = self.log.info
        else:
            log = self.log.debug
        log("qga exec: %s", " ".join(cmd))
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
        if self.guestos == "windows":
            return True
        if self.qga:
            v = self.qga_operational()
            if v:
                # qga is operational, but we have no generic method to ensure
                # the os is far enough in the boot for a encap start to succeed
                # (network, sssd, dockerd, ... may need to be started but we don't
                # know if they are managed by systemd, openrc, ...)
                time.sleep(self.qga_operational_delay)
            return v
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
        cmd = []
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

    def wait_for_shutdown(self):
        """
        Defines dedicated wait_for_shutdown that waits for `is_down` instead of `not is_up`.
        The `not is_up` is not enough, `in shutdown` can take time...
        TODO: Improve method with extra wait for not more `Id` when `State` is `shut off`

        Example of shutdown state transitions during shutdown:
            Thu Nov 27 10:24:02 CET 2025
            Id:             19
            ...
            State:          running
            ...

            Thu Nov 27 10:24:31 CET 2025
            Id:             19
            ...
            State:          in shutdown
            ...

            Thu Nov 27 10:24:33 CET 2025
            Id:             19
            ...
            State:          shut off
            ...

            Thu Nov 27 10:24:33 CET 2025
            Id:             -
            ...
            State:          shut off
            ...
        """
        def fn():
            if hasattr(self, "is_up_clear_cache"):
                getattr(self, "is_up_clear_cache")()
            return self.is_down()
        self.log.info("wait for down status")
        self.wait_for_fn(fn, self.stop_timeout, 2, errmsg="waited too long for shutdown")
