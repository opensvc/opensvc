from provisioning import Provisioning
from rcGlobalEnv import rcEnv
import os
import rcExceptions as ex

class ProvisioningLxc(Provisioning):
    config_template = """\
lxc.utsname = %(vm_name)s
lxc.tty = 4
lxc.pts = 1024
lxc.console = /opt/opensvc/log/%(vm_name)s.console.log

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
    def __init__(self, r):
        Provisioning.__init__(self, r)

        self.section = r.svc.config.defaults()
        self.rootfs = r.svc.config.get(r.rid, 'rootfs')
        self.template = r.svc.config.get(r.rid, 'template')

        # hostname file in the container rootfs
        self.p_hostname = os.path.join(self.rootfs, 'etc', 'hostname')

        self.vm_name = r.name

        # lxc root conf dir
        self.d_lxc = os.path.join(os.sep, 'var', 'lib', 'lxc')
        if not os.path.exists(self.d_lxc):
            self.d_lxc = os.path.join(os.sep, 'usr', 'local', 'var', 'lib', 'lxc')
        if not os.path.exists(self.d_lxc):
            self.d_lxc = None

        # container config file
        if self.d_lxc is not None:
            self.config = os.path.join(self.d_lxc, self.vm_name, 'config')

        # network config candidates
        self.interfaces = os.path.join(self.rootfs, 'etc', 'network', 'interfaces')
        self.network = os.path.join(self.rootfs, 'etc', 'sysconfig', 'network-scripts')

    def validate(self):
        if self.d_lxc is None:
            self.r.log.error("this node is not lxc capable")
            return True

        if not self.check_vm_name():
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
        f.write(self.config_template%dict(vm_name=self.vm_name, rootfs=self.rootfs))
        self.config = f.name
        f.close()

    def setup_lxc(self):
        if self.check_lxc():
            self.r.log.info("container is already created")
            return
        name = self.setup_lxc_config()
        with open("/opt/opensvc/log/%s.console.log"%self.vm_name, "a+") as f:
            f.write("")
        cmd = ['lxc-create', '-n', self.vm_name, '-f', self.config]
        (ret, out, err) = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def check_vm_name(self):
        if not os.path.exists(self.p_hostname):
            return False

        try:
            with open(self.p_hostname) as f:
                h = f.read().strip()
        except:
            self.r.log.error("can not get container hostname")
            raise ex.excError
    
        if h != self.vm_name:
            self.r.log.info("container hostname is not %s"%self.vm_name)
            return False

        return True

    def set_vm_name(self):
        if self.check_vm_name():
            self.r.log.info("container hostname already set")
            return
        with open(self.p_hostname, 'w') as f:
            f.write(self.vm_name+'\n')
        self.r.log.info("container hostname set to %s"%self.vm_name)

    def get_template(self):
        self.template_fname = os.path.basename(self.template)
        self.template_local = os.path.join(rcEnv.pathtmp, self.template_fname)
        if os.path.exists(self.template_local):
            self.r.log.info("template %s already downloaded"%self.template_fname)
            return
        import urllib
        fname, headers = urllib.urlretrieve(template, self.template_local)
        if 'invalid file' in headers.values():
            self.r.log.error("%s not found"%self.template)
            raise ex.excError

    def unpack_template(self):
        import tarfile
        os.chdir(self.rootfs)
        tar = tarfile.open(name=self.template_local, errorlevel=0)
        if os.path.exists(os.path.join(self.rootfs,'etc')):
            self.r.log.info("template already unpacked")
            return
        tar.extractall()
        tar.close()

    def setup_template(self):
        self.set_vm_name()

    def disable_udev(self):
        updaterc = os.path.join(self.rootfs, 'usr', 'sbin', 'update-rc.d')
        chkconfig = os.path.join(self.rootfs, 'sbin', 'chkconfig')
        if os.path.exists(updaterc):
            self.r.vcall(['chroot', self.rootfs, 'update-rc.d', '-f', 'udev', 'remove'])
        elif os.path.exists(chkconfig):
            self.r.vcall(['chroot', self.rootfs, 'chkconfig', 'udev', 'off'])

    def setup_getty(self):
         getty = os.path.join(self.rootfs, 'sbin', 'getty')
         if os.path.exists(getty):
             self.r.log.info("setup getty")
             inittab = os.path.join(self.rootfs, 'etc', 'inittab')
             with open(inittab, 'a') as f:
                 f.write("""
1:2345:respawn:/sbin/getty 38400 console
c1:12345:respawn:/sbin/getty 38400 tty1 linux
""")

    def setup_authkeys(self):
        pub = os.path.join(os.sep, 'root', '.ssh', 'id_dsa.pub')
        authkeys = os.path.join(self.rootfs, 'root', '.ssh', 'authorized_keys')
        if not os.path.exists(pub):
            self.r.log.error("no dsa found on node for root")
            return
        import shutil
        if not os.path.exists(os.path.dirname(authkeys)):
            os.makedirs(os.path.dirname(authkeys))
        shutil.copyfile(pub, authkeys)
        os.chmod(authkeys, int('600', 8))
        self.r.log.info("setup hypervisor root trust")

    def setup_ip(self, r):
        self.purge_known_hosts(r.addr)
        if os.path.exists(self.interfaces):
            return self.setup_ip_debian(r)
        elif os.path.exists(self.network):
            return self.setup_ip_rh(r)

    def setup_ip_rh(self, r):
        r.getaddr()
        buff = """
DEVICE=%(ipdev)s
IPADDR=%(ipaddr)s
NETMASK=%(netmask)s
ONBOOT=yes
"""%dict(ipdev=r.ipDev, netmask=r.mask, ipaddr=r.addr)
        intf = os.path.join(self.network, 'ifcfg-'+r.ipDev)
        with open(intf, 'w') as f:
            f.write(buff)

    def setup_ip_debian(self, r):
        r.getaddr()
        buff = """
auto lo
iface lo inet loopback

auto %(ipdev)s
iface %(ipdev)s inet static
    address %(ipaddr)s
    netmask %(netmask)s

"""%dict(ipdev=r.ipDev, netmask=r.mask, ipaddr=r.addr)
        with open(self.interfaces, 'w') as f:
            f.write(buff)

    def setup_ips(self):
        self.purge_known_hosts()
        for rs in self.r.svc.get_res_sets("ip"):
            for r in rs.resources:
                self.setup_ip(r)

    def purge_known_hosts(self, ip=None):
        if ip is None:
            cmd = ['ssh-keygen', '-R', self.r.svc.svcname]
        else:
            cmd = ['ssh-keygen', '-R', ip]
        ret, out, err = self.r.vcall(cmd, err_to_info=True)

    def provisioner(self):
        path = self.rootfs

        if not os.path.exists(path):
            os.makedirs(path)

        self.get_template()
        self.unpack_template()
        self.setup_template()
        self.setup_lxc()
        self.disable_udev()
        self.setup_getty()
        self.setup_authkeys()
        self.setup_ips()

        self.r.start()
        self.r.log.info("provisioned")
        return True
