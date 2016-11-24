import os
from datetime import datetime
from subprocess import *

import sys
import rcStatus
import resources as Res
from rcUtilitiesLinux import check_ping
from rcUtilities import which, justcall
from rcGlobalEnv import rcEnv
import resContainer
import rcExceptions as ex

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

    def files_to_sync(self):
        # the config file might be in a umounted fs resource
        # in which case, no need to ask for its sync as the sync won't happen
        l = []

        # replicate the config file in the system standard path
        cf = self.get_cf_path()
        if cf:
            l.append(cf)

        return l

    def rcp_from(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.excError()
        src = rootfs + src
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.excError()
        dst = rootfs + dst
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def lxc(self, action):
        self.find_cf()
        outf = '/var/tmp/svc_'+self.name+'_lxc_'+action+'.log'
        if action == 'start':
            cmd = ['lxc-start', '-d', '-n', self.name, '-o', outf, '-f', self.cf]
        elif action == 'stop':
            cmd = ['lxc-stop', '-n', self.name, '-o', outf]
        else:
            self.log.error("unsupported lxc action: %s" % action)
            return 1

        t = datetime.now()
        (ret, out, err) = self.vcall(cmd)
        len = datetime.now() - t
        self.log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
        if ret != 0:
            raise ex.excError

    def vm_hostname(self):
        if hasattr(self, "hostname"):
            return self.hostname
        try:
            self.hostname = self.get_cf_value("lxc.utsname")
        except:
            self.hostname = self.name
        if self.hostname is None:
            self.hostname = self.name
        return self.hostname

    def get_cf_value(self, param):
        self.find_cf()
        value = None
        if not os.path.exists(self.cf):
            return None
        with open(self.cf, 'r') as f:
            for line in f.readlines():
                if param not in line:
                    continue
                if line.strip()[0] == '#':
                    continue
                l = line.replace('\n', '').split('=')
                if len(l) < 2:
                    continue
                if l[0].strip() != param:
                    continue
                value = ' '.join(l[1:]).strip()
                break
        return value

    def get_rootfs(self):
        rootfs = self.get_cf_value("lxc.rootfs")
        if rootfs is None:
            self.log.error("could not determine lxc container rootfs")
            raise ex.excError
        return rootfs

    def install_drp_flag(self):
        rootfs = self.get_rootfs()
        flag = os.path.join(rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def set_cpuset_clone_children(self):
        ppath = "/sys/fs/cgroup/cpuset"
        if not os.path.exists(ppath):
            self.log.debug("set_clone_children: %s does not exist" % ppath)
            return
        path = "/sys/fs/cgroup/cpuset/lxc"
        val = "1"
        if not os.path.exists(path):
            self.log.info("mkdir %s" % path)
            os.makedirs(path)
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
            return
        if current_val == "1":
            self.log.debug("set_cpuset_clone_children: %s/%s already set to 1" % (path, parm))
            return
        self.set_sysfs(path, parm, "1")

    def get_sysfs(self, path, parm):
        fpath = os.sep.join([path, parm])
        if not os.path.exists(fpath):
            self.log.debug("get_sysfs: %s does not exist" % path)
            return
        with open(fpath, "r") as f:
            current_val = f.read().rstrip("\n")
        self.log.debug("get_sysfs: %s contains %s" % (fpath, repr(current_val)))
        return current_val

    def set_sysfs(self, path, parm, val):
        fpath = os.sep.join([path, parm])
        self.log.info("echo %s >%s" % (val, fpath))
        with open(fpath, "w") as f:
            f.write(val)

    def cleanup_cgroup(self, t="*"):
        import glob
        for p in glob.glob("/sys/fs/cgroup/%s/lxc/%s-[0-9]" % (t, self.name)) + \
                 glob.glob("/sys/fs/cgroup/%s/lxc/%s" % (t, self.name)):
            try:
                os.rmdir(p)
                self.log.info("removed leftover cgroup %s" % p)
            except Exception as e:
                self.log.debug("failed to remove leftover cgroup %s: %s" % (p, str(e)))

    def container_start(self):
        if not self.svc.create_pg:
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
        """ no harder way to stop a lxc container, raise to signal our
            helplessness
        """
        raise ex.excError

    def get_links(self):
        links = []
        cmd = ['lxc-info', '--name', self.name]
        out, err, ret = justcall(cmd)
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

    def is_up(self, nodename=None):
        if which("lxc-ps"):
            return self.is_up_ps(nodename=nodename)
        else:
            return self.is_up_info(nodename=nodename)

    def is_up_info(self, nodename=None):
        cmd = ['lxc-info', '--name', self.name]
        if nodename is not None:
            cmd = rcEnv.rsh.split() + [nodename] + cmd
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        if 'RUNNING' in out:
            return True
        return False

    def is_up_ps(self, nodename=None):
        cmd = ['lxc-ps', '--name', self.name]
        if nodename is not None:
            cmd = rcEnv.rsh.split() + [nodename] + cmd
        out, err, ret = justcall(cmd)
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
        cf = self.get_cf_path()
        if cf is None:
            self.log.debug("could not determine the config file standard hosting directory")
            return
        if self.cf == cf:
            return
        dn = os.path.dirname(cf)
        if not os.path.isdir(dn):
            try:
                os.makedirs(dn)
            except Exception as e:
                raise ex.excError("failed to create directory %s: %s"%(dn, str(e)))
        self.log.info("install %s as %s" % (self.cf, cf))
        try:
            import shutil
            shutil.copy(self.cf, cf)
        except Exception as e:
            raise ex.excError(str(e))

    def get_cf_path(self):
        path = which('lxc-info')
        if path is None:
            return None
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
        cf = self.get_cf_path()
        if cf is None:
            self.status_log("could not determine the config file standard hosting directory")
            return False
        if os.path.exists(cf):
            return True
        self.status_log("config file is not installed as %s" % cf)
        return False

    def _status(self, verbose=False):
        self.check_installed_cf()
        return resContainer.Container._status(self, verbose=verbose)

    def find_cf(self):
        if self.cf is not None:
            return

        d_lxc = os.path.join('var', 'lib', 'lxc')

        # seen on debian squeeze : prefix is /usr, but containers'
        # config files paths are /var/lib/lxc/$name/config
        # try prefix first, fallback to other know prefixes
        prefixes = [os.path.join(os.sep),
                    os.path.join(os.sep, 'usr'),
                    os.path.join(os.sep, 'usr', 'local')]
        for prefix in [self.prefix] + [p for p in prefixes if p != self.prefix]:
            cf = os.path.join(prefix, d_lxc, self.name, 'config')
            if os.path.exists(cf):
                cf_d = os.path.dirname(cf)
                if not os.path.exists(cf_d):
                    os.makedirs(cf_d)
                self.cf = cf
                return

        # on Oracle Linux, config is in /etc/lxc
        cf = os.path.join(os.sep, 'etc', 'lxc', self.name, 'config')
        if os.path.exists(cf):
            self.cf = cf
            return

        self.cf = None
        raise ex.excError("unable to find the container configuration file")

    def find_prefix(self):
        prefixes = [os.path.join(os.sep),
                    os.path.join(os.sep, 'usr'),
                    os.path.join(os.sep, 'usr', 'local')]
        for prefix in prefixes:
             if os.path.exists(os.path.join(prefix, 'bin', 'lxc-start')):
                 return prefix
        return None

    def __init__(self,
                 rid,
                 name,
                 guestos="Linux",
                 cf=None,
                 rcmd=None,
                 optional=False,
                 disabled=False,
                 monitor=False,
                 restart=0,
                 subset=None,
                 osvc_root_path=None,
                 tags=set([]),
                 always_on=set([])):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.lxc",
                                        guestos=guestos,
                                        optional=optional,
                                        disabled=disabled,
                                        monitor=monitor,
                                        restart=restart,
                                        subset=subset,
                                        osvc_root_path=osvc_root_path,
                                        tags=tags,
                                        always_on=always_on)

        if rcmd is not None:
            self.runmethod = rcmd
        elif which('lxc-attach') and os.path.exists('/proc/1/ns/pid'):
            self.runmethod = ['lxc-attach', '-n', name, '--']
        else:
            self.runmethod = rcEnv.rsh.split() + [name]

        if "lxc-attach" in ' '.join(self.runmethod):
            # override getaddr from parent class with a noop
            self.getaddr = self.dummy
        else:
            # enable ping test on start
            self.ping = self._ping

        self.cf = cf

    def dummy(self, cache_fallback=False):
        pass

    def on_add(self):
        self.prefix = self.find_prefix()
        if self.prefix is None:
            self.log.error("lxc install prefix not found")
            raise ex.excInitError

    def operational(self):
        if not resContainer.Container.operational(self):
            return False

        cmd = self.runmethod + ['test', '-f', '/bin/systemctl']
        out, err, ret = justcall(cmd)
        if ret == 1:
            # not a systemd container. no more checking.
            self.log.debug("/bin/systemctl not found in container")
            return True

        # systemd on-demand loading will let us start the encap service before
        # the network is fully initialized, causing start issues with nfs mounts
        # and listening apps.
        # => wait for systemd default target to become active
        cmd = self.runmethod + ['systemctl', 'is-active', 'default.target']
        out, err, ret = justcall(cmd)
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

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def provision(self):
        m = __import__("provLxc")
        prov = m.ProvisioningLxc(self)
        prov.provisioner()
