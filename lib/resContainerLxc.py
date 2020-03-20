"""
The lxc v1, v2, v3 resource driver
"""
import os
from datetime import datetime

import resources as Res
from rcUtilitiesLinux import check_ping
from rcUtilities import which, justcall, lazy, makedirs
from rcGlobalEnv import rcEnv
from svcBuilder import init_kwargs, container_kwargs, get_rcmd
from svcdict import KEYS
import resContainer
import rcExceptions as ex
import rcStatus

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
        "keyword": "mirror",
        "text": "Sets the ``MIRROR`` environment variable for :cmd:`lxc-create`, pointing the distribution server to use.",
        "provisioning": True
    },
    {
        "keyword": "security_mirror",
        "text": "Sets the ``SECURITY_MIRROR`` environment variable for :cmd:`lxc-create`, pointing the security distribution server to use. If not set but mirror is set, use mirror as the security mirror.",
        "provisioning": True
    },
    {
        "keyword": "rcmd",
        "convert": "shlex",
        "at": True,
        "example": "lxc-attach -e -n osvtavnprov01 -- ",
        "text": "An container remote command override the agent default"
    },
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
    kwargs["rcmd"] = get_rcmd(svc, s)
    kwargs["cf"] = svc.oget(s, "cf")
    kwargs["container_data_dir"] = svc.oget(s, "container_data_dir")
    r = Lxc(**kwargs)
    svc += r


class Lxc(resContainer.Container):
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
                 rid,
                 name,
                 guestos="Linux",
                 cf=None,
                 rcmd=None,
                 osvc_root_path=None,
                 container_data_dir=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.lxc",
                                        guestos=guestos,
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self.refresh_provisioned_on_provision = True
        self.refresh_provisioned_on_unprovision = True
        self.always_pg = True
        self.label = "lxc " + self.label
        self.container_data_dir = container_data_dir
        self.rcmd = rcmd
        self.links = None
        self.cf = cf

    def on_add(self):
        if self.rcmd is not None:
            self.runmethod = self.rcmd
        elif which('lxc-attach') and os.path.exists('/proc/1/ns/pid'):
            if self.lxcpath:
                self.runmethod = ['lxc-attach', '-n', self.name, '-P', self.lxcpath, '--']
            else:
                self.runmethod = ['lxc-attach', '-n', self.name, '--']
        else:
            self.runmethod = rcEnv.rsh.split() + [self.name]

        if "lxc-attach" in ' '.join(self.runmethod):
            # override getaddr from parent class with a noop
            self.getaddr = self.dummy
        else:
            # enable ping test on start
            self.ping = self._ping

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

    def rcp_from(self, src, dst):
        if not self.rootfs:
            raise ex.excError
        src = self.rootfs + src
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        if not self.rootfs:
            raise ex.excError
        dst = self.rootfs + dst
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
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
            raise ex.excError("unsupported lxc action: %s" % action)

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
            raise ex.excError

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
        rootfs = self.oget("rootfs")
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
            raise ex.excError("could not determine lxc container rootfs")
        if ":" in rootfs:
            # zfs:/tank/svc1, nbd:file1, dir:/foo ...
            rootfs = rootfs.split(":", 1)[-1]
        return rootfs

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
        return check_ping(self.addr, timeout=1)

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
            cmd = rcEnv.rsh.split() + [nodename] + cmd
        out, _, ret = justcall(cmd)
        if ret != 0:
            return False
        if 'RUNNING' in out:
            return True
        return False

    def is_up_ps(self, nodename=None):
        cmd = ['lxc-ps', '--name', self.name]
        if nodename is not None:
            cmd = rcEnv.rsh.split() + [nodename] + cmd
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
                raise ex.excError("failed to create directory %s: %s"%(cfg_d, str(exc)))
        self.log.info("install %s as %s", self.cf, cfg)
        try:
            import shutil
            shutil.copy(self.cf, cfg)
        except Exception as exc:
            raise ex.excError(str(exc))

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
        if res and res.status() not in (rcStatus.NA, rcStatus.UP, rcStatus.STDBY_UP):
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
        return resContainer.Container._status(self, verbose=verbose)

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
        raise ex.excError("unable to find the container configuration file")

    @lazy
    def prefix(self):
        prefixes = [os.path.join(os.sep),
                    os.path.join(os.sep, 'usr'),
                    os.path.join(os.sep, 'usr', 'local')]
        for prefix in prefixes:
            if os.path.exists(os.path.join(prefix, 'bin', 'lxc-start')):
                return prefix
        raise ex.excError("lxc install prefix not found")

    def dummy(self, cache_fallback=False):
        pass

    def operational(self):
        if not resContainer.Container.operational(self):
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
        resContainer.Container.start(self)
        self.svc.sub_set_action("ip", "start", tags=set([self.rid]))

    def stop(self):
        self.svc.sub_set_action("ip", "stop", tags=set([self.rid]))
        resContainer.Container.stop(self)

    def provision(self):
        resContainer.Container.provision(self)
        self.svc.sub_set_action("ip", "provision", tags=set([self.rid]))

    def unprovision(self):
        self.svc.sub_set_action("ip", "unprovision", tags=set([self.rid]))
        resContainer.Container.unprovision(self)

