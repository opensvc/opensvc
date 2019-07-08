import os
import shutil

import provisioning
from rcGlobalEnv import rcEnv
from rcUtilities import protected_dir, makedirs
import rcExceptions as ex

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

class Prov(provisioning.Prov):
    def validate(self):
        if self.d_lxc is None:
            self.r.log.error("this node is not lxc capable")
            return True

        if not self.check_hostname():
            return False

        if not self.check_lxc():
            self.r.log.error("container is not created")
            return False

        return True

    def check_lxc(self):
        if os.path.exists(self.r.cf):
            return True
        return False

    def setup_lxc_config(self):
        import tempfile
        f = tempfile.NamedTemporaryFile(delete=False)
        f.write(DEFAULT_CONFIG % dict(hostname=self.r.vm_hostname, rootfs=self.r.rootfs))
        self.config = f.name
        f.close()

    def setup_lxc(self):
        if self.check_lxc():
            self.r.log.info("container is already created")
            return
        self.setup_lxc_config()
        with open(os.path.join(rcEnv.paths.pathlog, "%s.console.log"%self.r.name), "a+") as f:
            f.write("")
        cmd = ['lxc-create', '-n', self.r.name, '-f', self.config]
        if self.r.lxcpath:
            makedirs(self.r.lxcpath)
            cmd += self.r.lxcpath_args
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def check_hostname(self):
        if not os.path.exists(self.p_hostname):
            return False

        try:
            with open(self.p_hostname) as f:
                h = f.read().strip()
        except:
            self.r.log.error("can not get container hostname")
            raise ex.excError

        if h != self.r.vm_hostname:
            self.r.log.info("container hostname is not %s"%self.r.vm_hostname)
            return False

        return True

    def set_hostname(self):
        if self.check_hostname():
            self.r.log.info("container hostname already set")
            return
        with open(self.p_hostname, 'w') as f:
            f.write(self.r.vm_hostname+'\n')
        self.r.log.info("container hostname set to %s"%self.r.vm_hostname)

    def get_template(self):
        self.template_fname = os.path.basename(self.template)
        self.template_local = os.path.join(rcEnv.paths.pathtmp, self.template_fname)
        if os.path.exists(self.template_local):
            self.r.log.info("template %s already downloaded"%self.template_fname)
            return
        import sys
        try:
            self.r.svc.node.urlretrieve(self.template, self.template_local)
        except IOError as e:
            self.r.log.error("download failed", ":", e)
            try:
                os.unlink(self.template_local)
            except:
                pass
            raise ex.excError

    def unpack_template(self):
        import tarfile
        os.chdir(self.r.rootfs)
        tar = tarfile.open(name=self.template_local, errorlevel=0)
        if os.path.exists(os.path.join(self.r.rootfs,'etc')):
            self.r.log.info("template already unpacked")
            return
        tar.extractall()
        tar.close()

    def setup_template(self):
        self.set_hostname()

    def disable_udev(self):
        updaterc = os.path.join(self.r.rootfs, 'usr', 'sbin', 'update-rc.d')
        chkconfig = os.path.join(self.r.rootfs, 'sbin', 'chkconfig')
        if os.path.exists(updaterc):
            self.r.vcall(['chroot', self.r.rootfs, 'update-rc.d', '-f', 'udev', 'remove'])
        elif os.path.exists(chkconfig):
            self.r.vcall(['chroot', self.r.rootfs, 'chkconfig', 'udev', 'off'])

    def setup_getty(self):
         getty = os.path.join(self.r.rootfs, 'sbin', 'getty')
         if os.path.exists(getty):
             self.r.log.info("setup getty")
             inittab = os.path.join(self.r.rootfs, 'etc', 'inittab')
             with open(inittab, 'a') as f:
                 f.write("""
1:2345:respawn:/sbin/getty 38400 console
c1:12345:respawn:/sbin/getty 38400 tty1 linux
""")

    def setup_authkeys(self):
        pub = os.path.join(os.sep, 'root', '.ssh', 'id_rsa.pub')
        authkeys = os.path.join(self.r.rootfs, 'root', '.ssh', 'authorized_keys')
        if not os.path.exists(pub):
            self.r.log.error("no rsa key found on node for root")
            return
        if not os.path.exists(os.path.dirname(authkeys)):
            os.makedirs(os.path.dirname(authkeys))
        shutil.copyfile(pub, authkeys)
        os.chmod(authkeys, int('600', 8))
        self.r.log.info("setup hypervisor root trust")

    def purge_known_hosts(self, ip=None):
        if ip is None:
            cmd = ['ssh-keygen', '-R', self.r.svc.name]
        else:
            cmd = ['ssh-keygen', '-R', ip]
        ret, out, err = self.r.vcall(cmd, err_to_info=True)

    def unprovisioner_shared_non_leader(self):
        self.purge_lxc_var()

    def unprovisioner(self):
        self.purge_lxc_var()

    def purge_lxc_var(self):
        self.set_d_lxc()
        path = os.path.join(self.d_lxc, self.r.name)
        if not os.path.exists(path):
            self.r.log.info("%s already cleaned up", path)
            return
        if protected_dir(path):
            self.r.log.warning("refuse to remove %s", path)
            return
        self.r.log.info("rm -rf %s", path)
        shutil.rmtree(path)

    def set_d_lxc(self):
        # lxc root conf dir
        if self.r.lxcpath:
            self.d_lxc = self.r.lxcpath
            return
        self.d_lxc = os.path.join(os.sep, 'var', 'lib', 'lxc')
        if not os.path.exists(self.d_lxc):
            self.d_lxc = os.path.join(os.sep, 'usr', 'local', 'var', 'lib', 'lxc')
        if not os.path.exists(self.d_lxc):
            self.d_lxc = None

    def provisioner(self):
        self.template = self.r.oget("template")

        # hostname file in the container rootfs
        self.p_hostname = os.path.join(self.r.rootfs, 'etc', 'hostname')
        self.set_d_lxc()

        if not os.path.exists(self.r.rootfs):
            os.makedirs(self.r.rootfs)

        if self.template is None or "://" not in self.template:
            self.provisioner_lxc_create()
        else:
            self.provisioner_archive()

    def provisioner_lxc_create(self):
        template_options = self.r.oget("template_options")
        cmd = ['lxc-create', '--name', self.r.name, "--dir", self.r.rootfs]
        if self.r.cf:
            cmd += ['-f', self.r.cf]
        if self.r.lxcpath:
            makedirs(self.r.lxcpath)
            cmd += self.r.lxcpath_args
            if not self.r.cf:
                cmd += ["-f", os.path.join(self.r.lxcpath, self.r.name, "config")]
        if self.template:
            cmd += ['--template', self.template]
            if template_options:
                cmd += ["--"] + template_options
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
        mirror = self.r.oget("mirror")
        if mirror:
            env["MIRROR"] = mirror
            env["SECURITY_MIRROR"] = mirror
        security_mirror = self.r.oget("security_mirror")
        if security_mirror:
            env["SECURITY_MIRROR"] = security_mirror
        ret, out, err = self.r.vcall(cmd, env=env)
        if ret != 0:
            raise ex.excError

    def provisioner_archive(self):
        # container config file
        if self.d_lxc is not None:
            self.config = os.path.join(self.d_lxc, self.r.name, 'config')

        self.get_template()
        self.unpack_template()
        self.setup_template()
        self.setup_lxc()
        self.disable_udev()
        self.setup_getty()
        self.setup_authkeys()

        self.r.start()
        self.r.log.info("provisioned")
        return True
