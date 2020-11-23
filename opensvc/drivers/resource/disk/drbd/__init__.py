import os
import re
import time

import core.exceptions as ex
import core.status
import daemon.handler
from core.comm import DEFAULT_DAEMON_TIMEOUT
from .. import BASE_KEYWORDS
from env import Env
from core.capabilities import capabilities
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.cache import cache
from utilities.lazy import lazy
from utilities.proc import justcall, call_log, which

RE_MINOR = r"^\s*device\s*/dev/drbd([0-9]+).*;"
RE_PORT = r"^\s*address.*:([0-9]+).*;"
RE_NODE_ID = r"^\s*node-id\s+([0-9]+)\s*;"
MAX_NODES = 32
MAX_DRBD = 512
MIN_PORT = 7289
MAX_PORT = 7489
DRIVER_GROUP = "disk"
DRIVER_BASENAME = "drbd"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "res",
        "required": True,
        "text": "The name of the drbd resource associated with this service "
                "resource. OpenSVC expect the resource configuration file to "
                "reside in ``/etc/drbd.d/resname.res``. The :c-res:`sync#i0` "
                "resource will take care of replicating this file to remote "
                "nodes."
    },
    {
        "keyword": "disk",
        "required": True,
        "provisioning": True,
        "text": "The path of the device to provision the drbd on."
    },
    {
        "keyword": "addr",
        "required": False,
        "provisioning": True,
        "text": "The addr to use to connect a peer. Use scoping to define "
                "each non-default address.",
        "default_text": "The ipaddr resolved for the nodename.",
    },
    {
        "keyword": "port",
        "required": False,
        "provisioning": True,
        "text": "The port to use to connect a peer. The default",
        "default_text": "A port free on all nodes, allocated by the agent.",
    },
]
DEPRECATED_SECTIONS = {
    "drbd": ["disk", "drbd"],
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
)


def driver_capabilities(node=None):
    data = []
    if which("drbdadm"):
        data.append("disk.drbd")
        out, err, ret = justcall(["drbdadm"])
        if "Version: 9" in out:
            out, err, ret = justcall(["modinfo", "drbd"])
            if ret == 0:
                for line in out.splitlines():
                    details = line.split()
                    if details[0] == 'version:' and details[1].startswith('9.'):
                        data.append("disk.drbd.mesh")
    return data


class PostConfigHandler(daemon.handler.BaseHandler):
    """
    Write a resource configuration file. Used by the provisioner to
    replicate the new configuration.
    """
    routes = (
        ("POST", "config"),
    )
    prototype = [
        {
            "name": "res",
            "desc": "The drbd resource name.",
            "required": True,
            "format": "string",
        },
        {
            "name": "data",
            "desc": "The drbd resource configuration file content.",
            "required": True,
            "format": "string",
        },
    ]
    access = {}

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        cf = "/etc/drbd.d/%s.res" % options.res
        with open(cf, "w") as f:
            f.write(options.data)


class GetConfigHandler(daemon.handler.BaseHandler):
    """
    Read a resource configuration file. Used by the provisioner to
    fetch an existing drbd resource configuration, for example
    when a new service instance provisions.
    """
    routes = (
        ("GET", "config"),
    )
    prototype = [
        {
            "name": "res",
            "desc": "The drbd resource name.",
            "required": True,
            "format": "string",
        },
    ]
    access = {}

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        cf = "/etc/drbd.d/%s.res" % options.res
        if not os.path.exists(cf):
            raise ex.HTTP(404, "resource configuration file does not exist")
        with open(cf, "r") as f:
            buff = f.read()
        return {"data": buff}


class AllocationsHandler(daemon.handler.BaseHandler):
    """
    Return the supported authentication methods.
    """
    routes = (
        ("GET", "allocations"),
    )
    prototype = []
    access = {}

    def action(self, nodename, thr=None, **kwargs):
        if "node.x.drbdadm" not in capabilities:
            raise ex.Error("this node is not drbd capable")
        out, err, ret = justcall(["drbdadm", "dump"])
        if ret:
            raise ex.HTTP(500, err)
        minors = set()
        ports = set()
        for line in out.splitlines():
            m = re.match(RE_MINOR, line)
            if m is not None:
                minors.add(int(m.group(1)))
            m = re.match(RE_PORT, line)
            if m is not None:
                ports.add(int(m.group(1)))
        return {
            "minors": sorted(list(minors)),
            "ports": sorted(list(ports)),
        }


DRIVER_HANDLERS = [
    AllocationsHandler,
    GetConfigHandler,
    PostConfigHandler,
]


class DiskDrbd(Resource):
    """ Drbd device resource

        The tricky part is that drbd devices can be used as PV
        and LV can be used as drbd base devices. Beware of the
        the ordering deduced from rids and subsets.

        Start 'ups' and promotes the drbd devices to primary.
        Stop 'downs' the drbd devices.
    """

    def __init__(self, res=None, disk=None, **kwargs):
        super(DiskDrbd, self).__init__(type="disk.drbd", **kwargs)
        self.res = res
        self.disk = disk
        self.label = "drbd %s" % res
        self.drbdadm = None
        self.rollback_even_if_standby = True
        self.can_rollback_role = False
        self.can_rollback_connection = False

    def __str__(self):
        return "%s resource=%s" % (super(DiskDrbd, self).__str__(), self.res)

    def files_to_sync(self):
        if os.path.exists(self.cf):
            return [self.cf]
        return []

    def drbdadm_cmd(self, cmd):
        if self.drbdadm is None:
            if "node.x.drbdadm" in capabilities:
                self.drbdadm = "drbdadm"
            else:
                raise ex.Error("drbdadm command not found")
        return [self.drbdadm] + cmd.split() + [self.res]

    @cache("drbdadm.dump.xml")
    def dump_xml(self):
        return justcall(["drbdadm", "dump-xml"])[0]

    def res_xml(self):
        from xml.etree.ElementTree import fromstring
        try:
            tree = fromstring(self.dump_xml())
        except Exception:
            return
        for res in tree.getiterator("resource"):
            if res.attrib["name"] != self.res:
                continue
            return res

    def exposed_devs(self):
        devps = set()
        res = self.res_xml()
        if res is None:
            return set()
        for host in res.getiterator("host"):
            if host.attrib["name"] != Env.nodename:
                continue
            d = host.find("device")
            if d is not None:
                devps |= set([d.text])
                continue
            for volume in res.getiterator("volume"):
                d = volume.find("device")
                if d is None:
                    continue
                devps |= set([d.text])
        return devps

    def sub_devs(self):
        devps = set()
        res = self.res_xml()
        if res is None:
            return set()
        for host in res.getiterator("host"):
            if host.attrib["name"] != Env.nodename:
                continue
            d = host.find("disk")
            if d is None:
                d = host.find("volume/disk")
            if d is None:
                continue
            devps |= set([d.text])
        return devps

    def state_changing_action(self, cmd, timeout=10):
        """
        State changing action can be denied by a peer node during
        commits. This method implement a retry loop waiting for
        the action to be not-denied.
        """
        self.log.info(" ".join(cmd))
        for i in range(timeout):
            out, err, ret = justcall(cmd)
            if ret == 11:
                # cluster-wide drbd state change in-progress
                time.sleep(1)
                continue
            elif ret != 0:
                call_log(buff=err, log=self.log, level="error")
                raise ex.Error()
            call_log(buff=out, log=self.log, level="info")
            return out, err, ret
        raise ex.Error("timeout waiting for action non-denied by peer")

    def drbdadm_adjust(self):
        cmd = self.drbdadm_cmd("adjust")
        self.vcall(cmd)

    def drbdadm_down_force(self):
        self.drbdadm_adjust()
        cmd = self.drbdadm_cmd("disconnect")
        self.state_changing_action(cmd)
        cmd = self.drbdadm_cmd("detach --force")
        self.state_changing_action(cmd)
        cmd = self.drbdadm_cmd("down")
        self.state_changing_action(cmd)
        self.svc.node.unset_lazy("devtree")

    def drbdadm_down(self):
        cmd = self.drbdadm_cmd("down")
        self.state_changing_action(cmd)
        self.svc.node.unset_lazy("devtree")

    def drbdadm_up(self):
        cmd = self.drbdadm_cmd("up")
        self.state_changing_action(cmd)
        self.wait_for_kwown_dstate()
        self.can_rollback_connection = True
        self.can_rollback = True

    def get_cstate(self):
        self.prereq()
        out, err, ret = justcall(self.drbdadm_cmd("cstate"))
        if ret != 0:
            if "Device minor not allocated" in err or ret == 10:
                return "Unattached"
            else:
                raise ex.Error
        return out.split("\n")[0].strip()

    def prereq(self):
        if not os.path.exists("/proc/drbd"):
            ret, out, err = self.vcall(["modprobe", "drbd"])
            if ret != 0:
                raise ex.Error

    def start_connection(self):
        cstate = self.get_cstate()
        if cstate == "Connected":
            self.log.info("drbd resource %s is already connected", self.res)
        elif cstate == "Connecting":
            self.log.info("drbd resource %s is already connecting", self.res)
        elif cstate == "StandAlone":
            self.drbdadm_down()
            self.drbdadm_up()
        elif cstate == "WFConnection":
            self.log.info("drbd resource %s peer node is not listening", self.res)
            pass
        else:
            self.log.info("cstate before connect: %s", cstate)
            self.drbdadm_up()

    def get_role(self):
        out, err, ret = justcall(self.drbdadm_cmd("role"))
        if ret != 0:
            raise ex.Error(err)
        out = out.strip()
        if out in ("Primary", "Secondary"):
            # drbd9
            return out
        try:
            loc, rem = out.split("\n")[0].split("/")
        except (IndexError, ValueError, AttributeError):
            raise ex.Error(out)
        return loc

    def start_role(self, role, extra_args=None):
        cur_role = self.get_role()
        if cur_role != role:
            if extra_args:
                cmd = self.drbdadm_cmd("%s %s" % (role.lower(), ' '.join(extra_args)))
            else:
                cmd = self.drbdadm_cmd(role.lower())
            self.state_changing_action(cmd)
            self.can_rollback_role = True
            self.can_rollback = True
        else:
            self.log.info("drbd resource %s is already %s", self.res, role)

    def startstandby(self):
        self.start_connection()
        role = self.get_role()
        if role == "Primary":
            return
        self.start_role("Secondary")

    def stopstandby(self):
        if not os.path.exists(self.cf):
            self.log.info("skip: resource not configured")
            return
        if not self.res_defined():
            self.log.info("skip: resource not defined (for this host)")
            return
        self.go_secondary()

    def go_secondary(self):
        self.start_connection()
        role = self.get_role()
        if role == "Secondary":
            return
        self.start_role("Secondary")

    def drbdadm_connect(self, discard_my_data=False):
        cmd1 = ["drbdadm", "--"]
        cmd2 = ["connect", self.res]
        if discard_my_data:
            cmd2 = ["--discard-my-data"] + cmd2
        ret, out, err = self.vcall(cmd1 + cmd2)
        if ret != 0:
            raise ex.Error

    def drbdadm_disconnect(self):
        cmd = ["drbdadm", "disconnect", self.res]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def start(self):
        if not os.path.exists(self.cf):
            self.log.info("skip: resource not configured")
            return
        if not self.res_defined():
            self.log.info("skip: resource not defined (for this host)")
            return
        self.start_connection()
        self.start_role("Primary")

    def stop(self):
        if not os.path.exists(self.cf):
            self.log.info("skip: resource not configured")
            return
        if not self.res_defined():
            self.log.info("skip: resource not defined (for this host)")
            return
        if self.is_standby and not self.svc.options.force:
            self.stopstandby()
        else:
            self.drbdadm_down()

    def shutdown(self):
        if not os.path.exists(self.cf):
            self.log.info("skip: resource not configured")
            return
        if not self.res_defined():
            self.log.info("skip: resource not defined (for this host)")
            return
        self.drbdadm_down()

    def rollback(self):
        if not self.can_rollback:
            return
        if self.is_standby:
            if not self.can_rollback_role:
                return
            self.go_secondary()
        else:
            if not self.can_rollback_connection:
                return
            self.drbdadm_down()

    def get_dstate(self):
        ret, out, err = self.call(self.drbdadm_cmd("dstate"))
        if ret != 0:
            raise ex.Error
        return out.splitlines()

    def wait_for_kwown_dstate(self):
        def check():
            for dstate in self.get_dstate():
                if dstate == "Diskless/DUnknown":
                    return False
            return True
        self.wait_for_fn(check, 5, 1, errmsg="waited too long for a known remote dstate")

    def dstate_uptodate(self, dstates):
        for dstate in dstates:
            if dstate != "UpToDate/UpToDate":
                return False
        return True

    def dstate_bootstraping(self, dstates):
        if len(dstates) != 1:
            return False
        return dstates[0] == "Inconsistent/DUnknown"

    def _status(self, verbose=False):
        try:
            role = self.get_role()
        except Exception as e:
            self.status_log(str(e))
            return core.status.DOWN
        self.status_log(str(role), "info")
        try:
            dstates = self.get_dstate()
        except ex.Error:
            self.status_log("drbdadm dstate %s failed" % self.res)
            return core.status.WARN
        if self.dstate_uptodate(dstates):
            pass
        else:
            status = None
            for idx, dstate in enumerate(dstates):
                dstatelist = dstate.split("/")
                dstateset = set(dstatelist)
                dstatelocal = dstatelist[0]
                if set(["UpToDate"]) == dstateset:
                    pass
                elif dstatelocal in ["Diskless", "DUnknown", "Unconfigured"]:
                    status = core.status.DOWN
                else:
                    self.status_log("unexpected drbd resource %s/%d state: %s" % (self.res, idx, dstate))
                # warnings
                if set(["Diskless", "DUnknown", "Unconfigured"]) & dstateset:
                    self.status_log("unexpected drbd resource %s/%d state: %s" % (self.res, idx, dstate))
            if status is not None:
                return status
        if role == "Primary":
            return core.status.UP
        elif role == "Secondary" and self.is_standby:
            return core.status.STDBY_UP
        else:
            return core.status.WARN

    def pre_provision_stop(self):
        """
        Skip normal stop before a unprovision.
        """
        pass

    def post_provision_start(self):
        """
        Skip normal start after a provision.
        """
        pass

    def provisioner(self):
        if self.svc.options.leader:
            self.write_config()
        elif not os.path.exists(self.cf) or not self.node_in_config():
            self.write_config_from_peer()
        self.create_md()
        self.drbdadm_down()
        self.drbdadm_up()
        if self.svc.options.leader:
            self.start_role("Primary", extra_args=["--force"])
            cstate = self.get_cstate()
        else:
            self.drbdadm_disconnect()
            self.drbdadm_connect()
        self.svc.node.unset_lazy("devtree")

    def unprovisioner(self):
        if not os.path.exists(self.cf):
            self.log.info("skip: resource not configured")
            return
        if self.res_defined():
            self.drbdadm_down_force()
            self.wipe_md()
        else:
            self.log.info("skip: resource not defined (for this host)")
        self.del_config()
        self.svc.node.unset_lazy("devtree")

    def res_defined(self):
        cmd = ["drbdadm", "--", "status", self.res]
        out, err, ret = justcall(cmd)
        if "not defined" in err:
            return False
        return True

    def has_md(self):
        cmd = ["drbdadm", "--", "--force", "dump-md", self.res]
        ret, out, err = self.call(cmd, errlog=False, outlog=False)
        if "No valid meta data found" in err:
            return False
        return True

    def create_md(self):
        if self.has_md():
            self.log.info("resource %s already has metadata" % self.res)
            return
        cmd = ["drbdadm", "create-md", "--force", self.res]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()

    def wipe_md(self):
        if not self.has_md():
            self.log.info("resource %s already has no metadata" % self.res)
            return
        cmd = ["drbdadm", "--", "--force", "wipe-md", self.res]
        self.log.info(" ".join(cmd))
        out, err, ret = justcall(cmd, input=b'yes\n')
        if ret == 20:
            # sub dev not found. no need to fail, as the sub dev is surely
            # flagged for unprovision too, which will wipe metadata.
            # this situation happens on unprovision on a stopped instance,
            # when drbd is stacked over another (stopped) disk resource.
            return
        if ret != 0:
            raise ex.Error(err)

    def res_create(self):
        if self.res_exists():
            self.log.info("resource %s already exists" % self.res)
            return
        cmd = ["drbdsetup", "new-resource", self.res]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()

    def res_delete(self):
        if not self.res_exists():
            self.log.info("resource %s already deleted" % self.res)
            return
        cmd = ["drbdsetup", "del-resource", self.res]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()

    def res_exists(self):
        cmd = ["drbdsetup", "show", self.res]
        ret, out, err = self.call(cmd)
        if ret != 20:
            return True
        return False

    def del_config(self):
        if not os.path.exists(self.cf):
            self.log.info("%s already deleted", self.res)
            return
        self.log.info("delete %s", self.cf)
        os.unlink(self.cf)

    def allocate_drbd(self):
        for idx in range(MAX_DRBD):
            if idx in self.allocations["minors"]:
                continue
            return "/dev/drbd%d" % idx
        raise ex.Error("no free minor")

    def allocate_port(self):
        for idx in range(MIN_PORT, MAX_PORT):
            if idx in self.allocations["ports"]:
                continue
            return idx
        raise ex.Error("no free minor")

    @lazy
    def cf(self):
        return "/etc/drbd.d/%s.res" % self.res

    def fetch_config(self):
        for node in self.svc.nodes:
            if node == Env.nodename:
                continue
            data = self.svc.daemon_get(
                {
                    "action": "/drivers/resource/disk/drbd/config",
                    "options": {"res": self.res}
                },
                node=node,
            )
            buff = data.get("nodes", {}).get(node, {}).get("data")
            if buff:
                return buff
        raise ex.Error("couldn't fetch the resource config from any peer")

    def write_config_from_peer(self):
        buff = self.fetch_config()
        need_replicate = False
        if not self.node_in_config(buff):
            buff = self.add_node_to_config(buff)
            need_replicate = True
        with open(self.cf, "w") as f:
            f.write(buff)
        if need_replicate:
            self.replicate_config(buff)

    def write_config(self):
        if os.path.exists(self.cf):
            self.log.info("%s already exists", self.cf)
            return
        lock_name = "drivers.resources.disk.drbd.allocate"
        lock_id = self.svc.node._daemon_lock(lock_name, timeout=120, on_error="raise")
        self.log.info("lock acquire: name=%s id=%s", lock_name, lock_id)
        try:
            self._write_config()
        finally:
            self.svc.node._daemon_unlock(lock_name, lock_id, timeout=120)
            self.log.info("lock released: name=%s id=%s", lock_name, lock_id)

    def _write_config(self):
        device = self.allocate_drbd()
        port = self.allocate_port()
        buff = self.format_config(device, port)
        with open(self.cf, "w") as filep:
            filep.write(buff)
        self.log.info("%s created", self.cf)
        self.unset_lazy("allocations")
        self.clear_cache("drbdadm.dump.xml")
        self.replicate_config(buff)

    def replicate_config(self, buff):
        data = self.svc.daemon_post(
            {
                "action": "/drivers/resource/disk/drbd/config",
                "options": {
                    "res": self.res,
                    "data": buff,
                },
            },
            node=[n for n in self.svc.nodes if n != Env.nodename],
            timeout=DEFAULT_DAEMON_TIMEOUT
        )
        if data.get("status", 1):
            raise ex.Error("failed to replicate config on nodes: %s" % data)

    @lazy
    def allocations(self):
        data = self.daemon_get_allocations()
        minors = set()
        ports = set()
        try:
            items = data["nodes"].items()
        except KeyError:
            raise ex.Error("unable to get current allocations: %s" % data.get("error"))
        for node, ndata in items:
            try:
                minors |= set(ndata["minors"])
                ports |= set(ndata["ports"])
            except KeyError:
                raise ex.Error("node %s invalid allocations report: %s" % (node, ndata.get("error", "")))
        data = {
            "minors": sorted(list(minors)),
            "ports": sorted(list(ports)),
        }
        return data

    def daemon_get_allocations(self):
        return self.svc.daemon_get(
            {"action": "/drivers/resource/disk/drbd/allocations"},
            node=self.svc.node.cluster_nodes,
        )

    @staticmethod
    def format_on(node, device, disk, addr, port, node_id=None):
        fmt_on =        "    on %s {\n%s    }\n"         # pep8: disable=E222
        fmt_on_device = "        device    %s;\n"        # pep8: disable=E222
        fmt_on_disk =   "        disk      %s;\n"        # pep8: disable=E222
        fmt_on_meta =   "        meta-disk internal;\n"  # pep8: disable=E222
        fmt_on_addr =   "        address   %s:%s;\n"     # pep8: disable=E222
        fmt_on_nid =    "        node-id   %d;\n"        # pep8: disable=E222
        buff_content = fmt_on_device % device
        buff_content += fmt_on_disk % disk
        buff_content += fmt_on_meta
        buff_content += fmt_on_addr % (addr, str(port))
        if node_id is not None:
            buff_content += fmt_on_nid % node_id
        buff_on = fmt_on % (node, buff_content)
        return buff_on

    def format_config(self, device, freeport):
        if self.has_capability("disk.drbd.mesh"):
            return self.format_config_v9(device, freeport)
        else:
            return self.format_config_v8(device, freeport)

    def format_config_v9(self, device, freeport):
        import socket
        fmt_res =       "resource %s {\n%s%s}\n"
        buff_on = ""
        for node_id, node in enumerate(self.svc.ordered_nodes):
            disk = self.oget("disk", impersonate=node)
            addr = self.oget("addr", impersonate=node) or socket.gethostbyname(node)
            port = self.oget("port", impersonate=node) or freeport
            buff_on += self.format_on(node, device, disk, addr, port, node_id=node_id)
        buff_mesh = "    connection-mesh {\n        hosts %s;\n    }\n" % " ".join(self.svc.ordered_nodes)
        buff = fmt_res % (self.res, buff_on, buff_mesh)
        return buff

    def format_config_v8(self, device, freeport):
        import socket
        fmt_res =       "resource %s {\n%s}\n"
        buff_on = ""
        for node in self.svc.ordered_nodes:
            disk = self.oget("disk", impersonate=node)
            addr = self.oget("addr", impersonate=node) or socket.gethostbyname(node)
            port = self.oget("port", impersonate=node) or freeport
            buff_on += self.format_on(node, device, disk, addr, port)
        buff = fmt_res % (self.res, buff_on)
        return buff

    def read_first_port(self, buff):
        for line in buff.splitlines():
            m = re.match(RE_PORT, line)
            if not m:
                continue
            return int(m.group(1))
        raise ex.Error("can not find the port in the current configuration")

    def read_first_device(self, buff):
        for line in buff.splitlines():
            m = re.match(RE_MINOR, line)
            if not m:
                continue
            return "/dev/drbd" + m.group(1)
        raise ex.Error("can not find the device in the current configuration")

    def read_next_node_id(self, buff):
        node_ids = []
        for line in buff.splitlines():
            m = re.match(RE_NODE_ID, line)
            if not m:
                continue
            node_ids.append(int(m.group(1)))
        for i in range(MAX_NODES):
            if i not in node_ids:
                return i
        raise ex.Error("can not find the device in the current configuration")

    def add_node_to_config(self, buff, node=None):
        node = node or Env.nodename
        import socket
        disk = self.oget("disk")
        addr = self.oget("addr") or socket.gethostbyname(node)
        port = self.oget("port") or self.read_first_port(buff)
        device = self.read_first_device(buff)
        node_id = self.read_next_node_id(buff)
        on_section = self.format_on(node, device, disk, addr, port, node_id)
        idx = buff.rindex("}")
        buff = "%s%s%s" % (buff[:idx], on_section, buff[idx:])
        buff = buff.replace("hosts ", "hosts %s " % node)
        return buff

    def node_in_config(self, buff=None, node=None):
        node = node or Env.nodename
        if buff is None:
            with open(self.cf, "r") as f:
                buff = f.read()
        pattern = r"^\s*on\s+%s\s*{" % node
        for line in buff.splitlines():
            if re.match(pattern, line):
                return True
        return False
