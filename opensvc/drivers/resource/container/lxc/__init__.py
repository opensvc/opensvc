"""
The lxc v1, v2, v3 resource driver
"""
import os
import shutil

from datetime import datetime

import core.exceptions as ex
import core.status
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
    KW_PROMOTE_RW, \
    KW_SCSIRESERV
from env import Env
from utilities.files import makedirs, protected_dir, rmtree_busy
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.proc import justcall, which

CAPABILITIES = {
    "cgroup_dir": "2.1",
}

DRIVER_GROUP = "container"
DRIVER_BASENAME = "lxc"
KEYWORDS = [
    {
        "keyword": "container_data_dir",
        "at": True,
        "text": "If this keyword is set, the service configures a resource-private container data store. This setup is allows stateful service relocalization.",
        "example": "/srv/svc1/data/containers"
    },
    {
        "keyword": "cf",
        "text": "Defines a lxc configuration file in a non-standard location.",
        "provisioning": True,
        "example": "/srv/mycontainer/config"
    },
    {
        "keyword": "rootfs",
        "text": "Sets the root fs directory of the container",
        "required": False,
        "provisioning": True
    },
    {
        "keyword": "template",
        "text": "Sets the url of the template unpacked into the container root fs or the name of the template passed to :cmd:`lxc-create`.",
        "required": True,
        "provisioning": True
    },
    {   
        "keyword": "template_options",
        "text": "The arguments to pass through :cmd:`lxc-create` to the per-template script.",
        "convert": "shlex",
        "default": [],
        "provisioning": True
    },
    {
        "keyword": "create_secrets_environment",
        "at": True,
        "provisioning": True,
        "convert": "shlex",
        "default": [],
        "text": "Set variables in the :cmd:`lxc-create` execution environment. A whitespace separated list of ``<var>=<secret name>/<key path>``. A shell expression spliter is applied, so double quotes can be around ``<secret name>/<key path>`` only or whole ``<var>=<secret name>/<key path>``. Variables are uppercased.",
        "example": "CRT=cert1/server.crt PEM=cert1/server.pem"
    },
    {
        "keyword": "create_configs_environment",
        "at": True,
        "provisioning": True,
        "convert": "shlex",
        "default": [],
        "text": "Set variables in the :cmd:`lxc-create` execution environment. The whitespace separated list of ``<var>=<config name>/<key path>``. A shell expression spliter is applied, so double quotes can be around ``<config name>/<key path>`` only or whole ``<var>=<config name>/<key path>``. Variables are uppercased.",
        "example": "CRT=cert1/server.crt PEM=cert1/server.pem"
    },
    {
        "keyword": "create_environment",
        "at": True,
        "provisioning": True,
        "convert": "shlex",
        "default": [],
        "text": "Set variables in the :cmd:`lxc-create` execution environment. The whitespace separated list of ``<var>=<config name>/<key path>``. A shell expression spliter is applied, so double quotes can be around ``<config name>/<key path>`` only or whole ``<var>=<config name>/<key path>``. Variables are uppercased.",
        "example": "CRT=cert1/server.crt PEM=cert1/server.pem"
    },
    {
        "keyword": "rcmd",
        "convert": "shlex",
        "at": True,
        "example": "lxc-attach -e -n osvtavnprov01 -- ",
        "text": "An container remote command override the agent default"
    },
    KW_START_TIMEOUT,
    KW_STOP_TIMEOUT,
    KW_NO_PREEMPT_ABORT,
    KW_NAME,
    KW_HOSTNAME,
    KW_OSVC_ROOT_PATH,
    KW_GUESTOS,
    KW_PROMOTE_RW,
    KW_SCSIRESERV,
]

DEFAULT_CONFIG = """\
lxc.utsname = %(hostname)s
lxc.tty = 4
lxc.pts = 1024
lxc.console = /var/log/opensvc/%(hostname)s.console.log

lxc.rootfs = %(rootfs)s
lxc.cgroup.devices.deny = a
# /dev/null and zero
lxc.cgroup.devices.allow = c 1:3 rwm
lxc.cgroup.devices.allow = c 1:5 rwm
# consoles
lxc.cgroup.devices.allow = c 5:1 rwm
lxc.cgroup.devices.allow = c 5:0 rwm
lxc.cgroup.devices.allow = c 4:0 rwm
lxc.cgroup.devices.allow = c 4:1 rwm
# /dev/{,u}random
lxc.cgroup.devices.allow = c 1:9 rwm
lxc.cgroup.devices.allow = c 1:8 rwm
lxc.cgroup.devices.allow = c 136:* rwm
lxc.cgroup.devices.allow = c 5:2 rwm
# rtc
lxc.cgroup.devices.allow = c 254:0 rwm

lxc.network.type = veth
lxc.network.flags = up
lxc.network.link = br0
lxc.network.name = eth0
lxc.network.mtu = 1500

# mounts point
lxc.mount.entry=proc %(rootfs)s/proc proc nodev,noexec,nosuid 0 0
lxc.mount.entry=devpts %(rootfs)s/dev/pts devpts defaults 0 0
lxc.mount.entry=sysfs %(rootfs)s/sys sysfs defaults 0 0
"""

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    data = []
    cmd = ["lxc-info", "--version"]
    out, _, ret = justcall(cmd)
    if ret == 0:
        data.append("container.lxc")
        version = out.strip()
        if version >= CAPABILITIES.get("cgroup_dir", "0"):
            data.append("container.lxc.cgroup_dir")
    return data


class ContainerLxc(BaseContainer):
    """
     container status transition diagram :
       ---------
      | STOPPED |<---------------
       ---------                 |
           |                     |
         start                   |
           |                     |
           V                     |
       ----------                |
      | STARTING |--error-       |
       ----------         |      |
           |              |      |
           V              V      |
       ---------    ----------   |
      | RUNNING |  | ABORTING |  |
       ---------    ----------   |
           |              |      |
      no process          |      |
           |              |      |
           V              |      |
       ----------         |      |
      | STOPPING |<-------       |
       ----------                |
           |                     |
            ---------------------
    """

    def __init__(self,
                 guestos="Linux",
                 cf=None,
                 rcmd=None,
                 rootfs=None,
                 container_data_dir=None,
                 template=None,
                 template_options=None,
                 create_environment=None,
                 create_configs_environment=None,
                 create_secrets_environment=None,
                 **kwargs):
        super(ContainerLxc, self).__init__(
            type="container.lxc",
            guestos=guestos,
            **kwargs
        )
        self.raw_rootfs = rootfs
        self.refresh_provisioned_on_provision = True
        self.refresh_provisioned_on_unprovision = True
        self.always_pg = True
        self.container_data_dir = container_data_dir
        self.rcmd = rcmd
        self.template = template
        self.template_options = template_options or []
        self.links = None
        self.cf = cf
        self.create_environment = create_environment
        self.create_configs_environment = create_configs_environment
        self.create_secrets_environment = create_secrets_environment

    def on_add(self):
        if "lxc-attach" in ' '.join(self.runmethod):
            # override getaddr from parent class with a noop
            self.getaddr = self.dummy
        else:
            # enable ping test on start
            self.ping = self._ping

    @lazy
    def label(self):  # pylint: disable=method-hidden
        return "lxc " + self.name

    @lazy
    def runmethod(self):
        if self.rcmd is not None:
            runmethod = self.rcmd
        elif which('lxc-attach') and os.path.exists('/proc/1/ns/pid'):
            if self.lxcpath:
                runmethod = ['lxc-attach', '-n', self.name, '-P', self.lxcpath, '--clear-env', '--']
            else:
                runmethod = ['lxc-attach', '-n', self.name, '--clear-env', '--']
        else:
            runmethod = Env.rsh.split() + [self.name]
        return runmethod

    @lazy
    def lxc_version(self):
        cmd = ["lxc-info", "--version"]
        out, _, _ = justcall(cmd)
        return out.strip()

    def capable(self, cap):
        if self.lxc_version >= CAPABILITIES.get(cap, "0"):
            return True
        return False

    def files_to_sync(self):
        # Don't synchronize container.lxc config in /var/lib/lxc if not shared
        # Non shared container resource mandates a private container for each
        # service instance, thus synchronizing the lxc config is counter productive
        # and can even lead to provisioning failure on secondary nodes.
        if not self.shared:
            return []

        # The config file might be in a umounted fs resource,
        # in which case, no need to ask for its sync as the sync won't happen
        self.find_cf()
        if not self.cf or not os.path.exists(self.cf):
            return []

        # The config file is hosted on a fs resource.
        # Let the user replicate it via a sync resource if the fs is not
        # shared. If the fs is shared, it must not be replicated to avoid
        # copying on the remote underlying fs (which may block a zfs dataset
        # mount).
        res = self.svc.resource_handling_file(self.cf)
        if res:
            return []

        # replicate the config file in the system standard path
        data = [self.cf]
        return data

    def enter(self):
        for cmd in [["/bin/bash"], ["/bin/sh"]]:
            try:
                os.system(" ".join(self.runmethod + cmd))
                return
            except ValueError:
                continue
            else:
                return

    def rcp_from(self, src, dst):
        if not self.rootfs:
            raise ex.Error
        src = self.rootfs + src
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        if not self.rootfs:
            raise ex.Error
        dst = self.rootfs + dst
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def lxc(self, action):
        self.find_cf()
        outf = None
        if action == 'start':
            outf = '/var/tmp/svc_'+self.name+'_lxc_'+action+'.log'
            if self.capable("cgroup_dir"):
                cmd = ["lxc-start", "-d", "-n", self.name, "-o", outf, "-s", "lxc.cgroup.dir="+self.cgroup_dir]
            else:
                cmd = ["lxc-start", "-d", "-n", self.name, "-o", outf]
            if self.cf:
                cmd += ['-f', self.cf]
            if self.lxcpath:
                makedirs(self.lxcpath)
                cmd += self.lxcpath_args
        elif action == 'stop':
            cmd = ['lxc-stop', '-n', self.name]
            cmd += self.lxcpath_args
        elif action == 'kill':
            cmd = ['lxc-stop', '--kill', '--name', self.name]
            cmd += self.lxcpath_args
        else:
            raise ex.Error("unsupported lxc action: %s" % action)

        def prex():
            os.umask(0o022)

        begin = datetime.now()
        ret, _, _ = self.vcall(cmd, preexec_fn=prex)
        duration = datetime.now() - begin
        loginfo = '%s done in %s - ret %i'%(action, duration, ret)
        if outf is not None:
            loginfo += ' - logs in %s'%outf
        self.log.info(loginfo)
        if ret != 0:
            raise ex.Error

    def get_cf_value(self, param):
        self.find_cf()
        value = None
        if not os.path.exists(self.cf):
            return None
        with open(self.cf, 'r') as ofile:
            for line in ofile.readlines():
                if param not in line:
                    continue
                if line.strip()[0] == '#':
                    continue
                data = line.replace('\n', '').split('=')
                if len(data) < 2:
                    continue
                if data[0].strip() != param:
                    continue
                value = ' '.join(data[1:]).strip()
                break
        return value

    @lazy
    def rootfs(self):
        rootfs = self.raw_rootfs
        if rootfs:
            rootfs, vol = self.replace_volname(rootfs, strict=False)
        if rootfs:
            return rootfs
        if self.lxcpath:
            return os.path.join(self.lxcpath, "config")
        rootfs = self.get_cf_value("lxc.rootfs")
        if rootfs is None:
            rootfs = self.get_cf_value("lxc.rootfs.path")
        if rootfs is None:
            raise ex.Error("could not determine lxc container rootfs")
        if ":" in rootfs:
            # zfs:/tank/svc1, nbd:file1, dir:/foo ...
            rootfs = rootfs.split(":", 1)[-1]
        return rootfs

    @property
    def zonepath(self):
        return self.rootfs

    @lazy
    def lxcpath(self):
        if self.container_data_dir:
            path, _ = self.replace_volname(self.container_data_dir, strict=False, errors="ignore")
            return path

    @lazy
    def lxcpath_args(self):
        if self.lxcpath:
            return ["-P", self.lxcpath]
        return []

    def install_drp_flag(self):
        flag = os.path.join(self.rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s", flag)
        with open(flag, 'w') as ofile:
            ofile.write(' ')

    def set_cpuset_clone_children(self):
        ppath = "/sys/fs/cgroup/cpuset"
        if not os.path.exists(ppath):
            self.log.debug("set_clone_children: %s does not exist", ppath)
            return
        if self.capable("cgroup_dir"):
            path = os.path.join(ppath, self.cgroup_dir)
        else:
            path = os.path.join(ppath, "lxc")
        try:
            os.makedirs(path)
            self.log.info("mkdir %s", path)
        except (OSError, IOError):
            # errno 17: file exists
            pass
        paths = [path]
        while path != ppath:
            path = os.path.dirname(path)
            paths.append(path)
        for path in sorted(paths):
            for parm in ("cpuset.mems", "cpuset.cpus"):
                current_val = self.get_sysfs(path, parm)
                if current_val is None:
                    continue
                if current_val == "":
                    parent_val = self.get_sysfs(ppath, parm)
                    self.set_sysfs(path, parm, parent_val)
            parm = "cgroup.clone_children"
            current_val = self.get_sysfs(path, parm)
            if current_val is None:
                continue
            if current_val == "1":
                self.log.debug("set_cpuset_clone_children: %s/%s already set to 1", path, parm)
                continue
            self.set_sysfs(path, parm, "1")

    def get_sysfs(self, path, parm):
        fpath = os.sep.join([path, parm])
        if not os.path.exists(fpath):
            self.log.debug("get_sysfs: %s does not exist", path)
            return None
        with open(fpath, "r") as ofile:
            current_val = ofile.read().rstrip("\n")
        self.log.debug("get_sysfs: %s contains %s", fpath, repr(current_val))
        return current_val

    def set_sysfs(self, path, parm, val):
        fpath = os.sep.join([path, parm])
        self.log.info("echo %s >%s", val, fpath)
        with open(fpath, "w") as ofile:
            ofile.write(val)

    def cleanup_cgroup(self, grp="*"):
        import glob
        for path in glob.glob("/sys/fs/cgroup/%s/lxc/%s-[0-9]" % (grp, self.name)) + \
                    glob.glob("/sys/fs/cgroup/%s/lxc/%s" % (grp, self.name)):
            try:
                os.rmdir(path)
                self.log.info("removed leftover cgroup %s", path)
            except Exception as exc:
                self.log.debug("failed to remove leftover cgroup %s: %s", path, str(exc))

    def container_start(self):
        self.cleanup_cgroup()
        self.set_cpuset_clone_children()
        self.install_cf()
        self.lxc('start')

    def container_stop(self):
        self.links = self.get_links()
        self.install_cf()
        self.lxc('stop')

    def post_container_stop(self):
        self.cleanup_links(self.links)
        self.cleanup_cgroup()

    def container_forcestop(self):
        self.lxc('kill')

    def get_pid(self):
        cmd = ['lxc-info', '--name', self.name, '-p']
        cmd += self.lxcpath_args
        out, _, ret = justcall(cmd)
        if ret != 0:
            return
        try:
            return int(out.split()[-1])
        except IndexError:
            return

    def get_links(self):
        links = []
        cmd = ['lxc-info', '--name', self.name]
        cmd += self.lxcpath_args
        out, _, ret = justcall(cmd)
        if ret != 0:
            return []
        for line in out.splitlines():
            if line.startswith("Link:"):
                links.append(line.split()[-1].strip())
        return links

    def cleanup_link(self, link):
        cmd = ["ip", "link", "del", "dev", link]
        out, err, ret = justcall(cmd)
        if ret == 0:
            self.log.info(" ".join(cmd))
        else:
            self.log.debug(" ".join(cmd)+out+err)

    def cleanup_links(self, links):
        for link in links:
            self.cleanup_link(link)

    def _ping(self):
        return utilities.ping.check_ping(self.addr, timeout=1)

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    @lazy
    def cgroup_dir(self):
        return self.svc.pg.get_cgroup_relpath(self)

    def is_up(self, nodename=None):
        if which("lxc-ps"):
            return self.is_up_ps(nodename=nodename)
        return self.is_up_info(nodename=nodename)

    def is_up_info(self, nodename=None):
        cmd = ['lxc-info', '--name', self.name]
        cmd += self.lxcpath_args
        if nodename is not None:
            cmd = Env.rsh.split() + [nodename] + cmd
        out, _, ret = justcall(cmd)
        if ret != 0:
            return False
        if 'RUNNING' in out:
            return True
        return False

    def is_up_ps(self, nodename=None):
        cmd = ['lxc-ps', '--name', self.name]
        if nodename is not None:
            cmd = Env.rsh.split() + [nodename] + cmd
        out, _, ret = justcall(cmd)
        if ret != 0:
            return False
        if self.name in out:
            return True
        return False

    def get_container_info(self):
        cpu_set = self.get_cf_value("lxc.cgroup.cpuset.cpus")
        if cpu_set is None:
            vcpus = 0
        else:
            vcpus = len(cpu_set.split(','))
        return {'vcpus': str(vcpus), 'vmem': '0'}

    def check_manual_boot(self):
        return True

    def check_capabilities(self):
        if not which('lxc-info'):
            self.log.debug("lxc-info is not in PATH")
            return False
        return True

    def install_cf(self):
        self.find_cf()
        if self.cf is None:
            return
        cfg = self.get_cf_path()
        if cfg is None:
            self.log.debug("could not determine the config file standard hosting directory")
            return
        if self.cf == cfg:
            return
        cfg_d = os.path.dirname(cfg)
        if not os.path.isdir(cfg_d):
            try:
                os.makedirs(cfg_d)
            except Exception as exc:
                raise ex.Error("failed to create directory %s: %s"%(cfg_d, str(exc)))
        self.log.info("install %s as %s", self.cf, cfg)
        try:
            shutil.copy(self.cf, cfg)
        except Exception as exc:
            raise ex.Error(str(exc))

    def get_cf_path(self):
        if self.lxcpath:
            return os.path.join(self.lxcpath, self.name, "config")
        path = which('lxc-info')
        if path is None:
            return
        dpath = os.path.dirname(path)
        if not dpath.endswith("bin"):
            return
        dpath = os.path.realpath(os.path.join(dpath, ".."))

        if dpath in (os.sep, "/usr") and os.path.exists("/var/lib/lxc"):
            return "/var/lib/lxc/%s/config" % self.name
        if dpath in ("/usr/local") and os.path.exists("/usr/local/var/lib/lxc"):
            return "/usr/local/var/lib/lxc/%s/config" % self.name
        if dpath in (os.sep, "/usr") and os.path.exists("/etc/lxc"):
            return "/etc/lxc/%s/config" % self.name

    def check_installed_cf(self):
        cfg = self.get_cf_path()
        if not self.is_provisioned():
            return True
        res = self.svc.resource_handling_file(cfg)
        if res and res.status() not in (core.status.NA, core.status.UP, core.status.STDBY_UP):
            return True
        if cfg is None:
            self.status_log("could not determine the config file standard hosting directory")
            return False
        if os.path.exists(cfg):
            return True
        self.status_log("config file is not installed as %s" % cfg)
        return False

    def _status(self, verbose=False):
        self.check_installed_cf()
        return super(ContainerLxc, self)._status(verbose=verbose)

    def find_cf(self):
        if self.cf is not None:
            self.cf, vol = self.replace_volname(self.cf, strict=False)
            return

        if self.lxcpath:
            d_lxc = self.lxcpath
            self.cf = os.path.join(d_lxc, self.name, 'config')
            return

        d_lxc = os.path.join('var', 'lib', 'lxc')

        # seen on debian squeeze : prefix is /usr, but containers'
        # config files paths are /var/lib/lxc/$name/config
        # try prefix first, fallback to other know prefixes
        prefixes = [os.path.join(os.sep),
                    os.path.join(os.sep, 'usr'),
                    os.path.join(os.sep, 'usr', 'local')]
        for prefix in [self.prefix] + [p for p in prefixes if p != self.prefix]:
            cfg = os.path.join(prefix, d_lxc, self.name, 'config')
            if os.path.exists(cfg):
                cfg_d = os.path.dirname(cfg)
                makedirs(cfg_d)
                self.cf = cfg
                return

        # on Oracle Linux, config is in /etc/lxc
        cfg = os.path.join(os.sep, 'etc', 'lxc', self.name, 'config')
        if os.path.exists(cfg):
            self.cf = cfg
            return

        self.cf = None
        raise ex.Error("unable to find the container configuration file")

    @lazy
    def prefix(self):
        prefixes = [os.path.join(os.sep),
                    os.path.join(os.sep, 'usr'),
                    os.path.join(os.sep, 'usr', 'local')]
        for prefix in prefixes:
            if os.path.exists(os.path.join(prefix, 'bin', 'lxc-start')):
                return prefix
        raise ex.Error("lxc install prefix not found")

    def dummy(self, cache_fallback=False):
        pass

    def operational(self):
        if not super(ContainerLxc, self).operational():
            return False

        cmd = self.runmethod + ['test', '-f', '/bin/systemctl']
        out, _, ret = justcall(cmd)
        if ret == 1:
            # not a systemd container. no more checking.
            self.log.debug("/bin/systemctl not found in container")
            return True

        # systemd on-demand loading will let us start the encap service before
        # the network is fully initialized, causing start issues with nfs mounts
        # and listening apps.
        # => wait for systemd default target to become active
        cmd = self.runmethod + ['systemctl', 'is-active', 'default.target']
        out, _, ret = justcall(cmd)
        if ret == 1:
            # if systemctl is-active fails, retry later
            self.log.debug("systemctl is-active failed")
            return False
        if out.strip() == "active":
            self.log.debug("systemctl is-active succeeded")
            return True

        # ok, wait some more
        self.log.debug("waiting for lxc to come up")
        return False

    def cni_containerid(self):
        """
        Used by ip.cni
        """
        return self.name

    def cni_netns(self):
        """
        Used by ip.cni and ip.netns
        """
        try:
            return "/proc/%d/ns/net" % self.get_pid()
        except (TypeError, ValueError):
            return

    def start(self):
        super(ContainerLxc, self).start()
        self.svc.sub_set_action("ip", "start", tags=set([self.rid]))
        self.svc.sub_set_action("disk.scsireserv", "start", tags=set([self.name]))
        self.svc.sub_set_action("disk.zpool", "start", tags=set([self.name]))
        self.svc.sub_set_action("fs", "start", tags=set([self.name]))

    def stop(self):
        self.svc.sub_set_action("fs", "stop", tags=set([self.name]))
        self.svc.sub_set_action("disk.zpool", "stop", tags=set([self.name]))
        self.svc.sub_set_action("disk.scsireserv", "stop", tags=set([self.name]))
        self.svc.sub_set_action("ip", "stop", tags=set([self.rid]))
        super(ContainerLxc, self).stop()

    def provision(self):
        super(ContainerLxc, self).provision()
        self.svc.sub_set_action("ip", "provision", tags=set([self.rid]))
        self.svc.sub_set_action("disk.scsireserv", "provision", tags=set([self.name]))
        self.svc.sub_set_action("disk.zpool", "provision", tags=set([self.name]))
        self.svc.sub_set_action("fs", "provision", tags=set([self.name]))

    def unprovision(self):
        self.svc.sub_set_action("fs", "unprovision", tags=set([self.name]))
        self.svc.sub_set_action("disk.zpool", "unprovision", tags=set([self.name]))
        self.svc.sub_set_action("disk.scsireserv", "unprovision", tags=set([self.name]))
        self.svc.sub_set_action("ip", "unprovision", tags=set([self.rid]))
        super(ContainerLxc, self).unprovision()




    def validate(self):
        if self.d_lxc is None:
            self.log.error("this node is not lxc capable")
            return True

        if not self.check_hostname():
            return False

        if not self.check_lxc():
            self.log.error("container is not created")
            return False

        return True

    def check_lxc(self):
        if os.path.exists(self.cf):
            return True
        return False

    def setup_lxc_config(self):
        import tempfile
        f = tempfile.NamedTemporaryFile(delete=False)
        f.write(DEFAULT_CONFIG % dict(hostname=self.vm_hostname, rootfs=self.rootfs))
        self.config = f.name
        f.close()

    def setup_lxc(self):
        if self.check_lxc():
            self.log.info("container is already created")
            return
        self.setup_lxc_config()
        with open(os.path.join(Env.paths.pathlog, "%s.console.log"%self.name), "a+") as f:
            f.write("")
        cmd = ['lxc-create', '-n', self.name, '-f', self.config]
        if self.lxcpath:
            makedirs(self.lxcpath)
            cmd += self.lxcpath_args
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def check_hostname(self):
        if not os.path.exists(self.p_hostname):
            return False

        try:
            with open(self.p_hostname) as f:
                h = f.read().strip()
        except:
            self.log.error("can not get container hostname")
            raise ex.Error

        if h != self.vm_hostname:
            self.log.info("container hostname is not %s"%self.vm_hostname)
            return False

        return True

    def set_hostname(self):
        if self.check_hostname():
            self.log.info("container hostname already set")
            return
        with open(self.p_hostname, 'w') as f:
            f.write(self.vm_hostname+'\n')
        self.log.info("container hostname set to %s"%self.vm_hostname)

    def get_template(self):
        self.template_fname = os.path.basename(self.template)
        self.template_local = os.path.join(Env.paths.pathtmp, self.template_fname)
        if os.path.exists(self.template_local):
            self.log.info("template %s already downloaded"%self.template_fname)
            return
        from utilities.uri import Uri
        secure = self.svc.node.oget("node", "secure_fetch")
        try:
            with Uri(self.template, secure=secure).fetch() as fpath:
                shutil.copy(fpath, self.template_local)
        except IOError as e:
            self.log.error("download failed", ":", e)
            raise ex.Error

    def unpack_template(self):
        import tarfile
        os.chdir(self.rootfs)
        tar = tarfile.open(name=self.template_local, errorlevel=0)
        if os.path.exists(os.path.join(self.rootfs,'etc')):
            self.log.info("template already unpacked")
            return
        tar.extractall()
        tar.close()

    def setup_template(self):
        self.set_hostname()

    def disable_udev(self):
        updaterc = os.path.join(self.rootfs, 'usr', 'sbin', 'update-rc.d')
        chkconfig = os.path.join(self.rootfs, 'sbin', 'chkconfig')
        if os.path.exists(updaterc):
            self.vcall(['chroot', self.rootfs, 'update-rc.d', '-f', 'udev', 'remove'])
        elif os.path.exists(chkconfig):
            self.vcall(['chroot', self.rootfs, 'chkconfig', 'udev', 'off'])

    def setup_getty(self):
         getty = os.path.join(self.rootfs, 'sbin', 'getty')
         if os.path.exists(getty):
             self.log.info("setup getty")
             inittab = os.path.join(self.rootfs, 'etc', 'inittab')
             with open(inittab, 'a') as f:
                 f.write("""
1:2345:respawn:/sbin/getty 38400 console
c1:12345:respawn:/sbin/getty 38400 tty1 linux
""")

    def setup_authkeys(self):
        pub = os.path.join(os.sep, 'root', '.ssh', 'id_rsa.pub')
        authkeys = os.path.join(self.rootfs, 'root', '.ssh', 'authorized_keys')
        if not os.path.exists(pub):
            self.log.error("no rsa key found on node for root")
            return
        if not os.path.exists(os.path.dirname(authkeys)):
            os.makedirs(os.path.dirname(authkeys))
        shutil.copyfile(pub, authkeys)
        os.chmod(authkeys, int('600', 8))
        self.log.info("setup hypervisor root trust")

    def purge_known_hosts(self, ip=None):
        if ip is None:
            cmd = ['ssh-keygen', '-R', self.svc.name]
        else:
            cmd = ['ssh-keygen', '-R', ip]
        ret, out, err = self.vcall(cmd, err_to_info=True)

    def unprovisioner_shared_non_leader(self):
        self.purge_lxc_var()

    def unprovisioner(self):
        self.purge_lxc_var()

    def purge_lxc_var(self):
        self.set_d_lxc()
        path = os.path.join(self.d_lxc, self.name)
        if not os.path.exists(path):
            self.log.info("%s already cleaned up", path)
            return
        if protected_dir(path):
            self.log.warning("refuse to remove %s", path)
            return
        self.log.info("rm -rf %s", path)
        rmtree_busy(path)

    def set_d_lxc(self):
        # lxc root conf dir
        if self.lxcpath:
            self.d_lxc = self.lxcpath
            return
        self.d_lxc = os.path.join(os.sep, 'var', 'lib', 'lxc')
        if not os.path.exists(self.d_lxc):
            self.d_lxc = os.path.join(os.sep, 'usr', 'local', 'var', 'lib', 'lxc')
        if not os.path.exists(self.d_lxc):
            self.d_lxc = None

    def provisioner(self):
        # hostname file in the container rootfs
        self.p_hostname = os.path.join(self.rootfs, 'etc', 'hostname')
        self.set_d_lxc()

        if not os.path.exists(self.rootfs):
            os.makedirs(self.rootfs)

        if self.template is None or "://" not in self.template:
            self.provisioner_lxc_create()
        else:
            self.provisioner_archive()

    def provisioner_lxc_create(self):
        cmd = ['lxc-create', '--name', self.name, "--dir", self.rootfs]
        if self.cf and os.path.exists(self.cf):
            cmd += ['-f', self.cf]
        if self.lxcpath:
            makedirs(self.lxcpath)
            cmd += self.lxcpath_args
            if not self.cf:
                cf = os.path.join(self.lxcpath, self.name, "config")
                if os.path.exists(cf):
                    cmd += ["-f", cf]
        if self.template:
            cmd += ['--template', self.template]
            if self.template_options:
                cmd += ["--"] + self.template_options
        env = {
            "DEBIAN_FRONTEND": "noninteractive",
            "DEBIAN_PRIORITY": "critical",
        }
        for key in ("http_proxy", "https_proxy", "ftp_proxy", "rsync_proxy"):
            if key in os.environ:
                env[key] = os.environ[key]
            key = key.upper()
            if key in os.environ:
                env[key] = os.environ[key]
        env.update(self.get_create_env())
        self.log.info(" ".join(cmd))
        ret = self.lcall(cmd, env=env)
        if ret != 0:
            raise ex.Error

    def get_create_env(self):
        env = {}
        env.update(self.direct_environment_env(self.create_environment))
        env.update(self.kind_environment_env("cfg", self.create_configs_environment))
        env.update(self.kind_environment_env("sec", self.create_secrets_environment))
        return env

    def provisioner_archive(self):
        # container config file
        if self.d_lxc is not None:
            self.config = os.path.join(self.d_lxc, self.name, 'config')

        self.get_template()
        self.unpack_template()
        self.setup_template()
        self.setup_lxc()
        self.disable_udev()
        self.setup_getty()
        self.setup_authkeys()

        self.start()
        self.log.info("provisioned")
        return True

    def provisioned(self):
        cmd = ['lxc-info', '--name', self.name, '-p']
        cmd += self.lxcpath_args
        out, _, ret = justcall(cmd)
        if ret != 0:
            return False
        return True
