import provisioning
from rcGlobalEnv import rcEnv
from rcUtilities import which, lazy
import os
import rcExceptions as ex

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)
        self.virtinst_cfdisk = []

    @lazy
    def snapof(self):
        return self.r.svc.oget(self.r.rid, "snapof")

    @lazy
    def snap(self):
        return self.r.svc.oget(self.r.rid, "snap")

    @lazy
    def virtinst(self):
        return self.r.svc.oget(self.r.rid, "virtinst")

    def check_kvm(self):
        if os.path.exists(self.r.cf):
            return True
        return False

    def setup_kvm(self):
        if self.virtinst is None:
            self.r.log.error("the 'virtinst' parameter must be set")
            raise ex.excError
        cmd = [] + self.virtinst + self.virtinst_cfdisk
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def setup_ips(self):
        self.purge_known_hosts()
        for resource in self.r.svc.get_resources("ip"):
            self.purge_known_hosts(resource.addr)

    def purge_known_hosts(self, ip=None):
        if ip is None:
            cmd = ['ssh-keygen', '-R', self.r.svc.name]
        else:
            cmd = ['ssh-keygen', '-R', ip]
        ret, out, err = self.r.vcall(cmd, err_to_info=True)

    def setup_snap(self):
        if self.snap is None and self.snapof is None:
            return
        elif self.snap and self.snapof is None:
            self.r.log.error("the 'snapof' parameter is required when 'snap' parameter present")
            raise ex.excError
        elif self.snapof and self.snap is None:
            self.r.log.error("the 'snap' parameter is required when 'snapof' parameter present")
            raise ex.excError

        if not which('btrfs'):
            self.r.log.error("'btrfs' command not found")
            raise ex.excError

        cmd = ['btrfs', 'subvolume', 'snapshot', self.snapof, self.snap]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def get_pubkey(self):
        pub = ""
        key_types = ['dsa', 'rsa']
        for key_type in key_types:
            p = os.path.join(os.sep, 'root', '.ssh', 'id_%s.pub' % key_type)
            try:
                self.r.log.info("try use root public key: %s", p)
                with open(p) as f:
                    pub = f.read(8000)
                break
            except:
                pass
        if not pub:
            self.r.log.error('failed to read root public key')
            raise ex.excError
        return pub

    def get_gw(self):
        cmd = ['route', '-n']
        ret, out, err = self.r.call(cmd)
        if ret != 0:
            self.r.log.error('failed to read routing table')
            raise ex.excError
        for line in out.split('\n'):
            if line.startswith('0.0.0.0'):
                l = line.split()
                if len(l) > 1:
                    return l[1]
        self.r.log.error('failed to find default gateway')
        raise ex.excError

    def get_ns(self):
        p = os.path.join(os.sep, 'etc', 'resolv.conf')
        with open(p) as f:
            for line in f.readlines():
                if 'nameserver' in line:
                    l = line.split()
                    if len(l) > 1:
                        return l[1]
        self.r.log.error('failed to find a nameserver')
        raise ex.excError

    def get_config(self):
        cf = ['todo']
        s = ';'.join(('vm', self.r.name))
        cf.append(s)
        s = 'ns;192.168.122.1'
        cf.append(s)
        s = ';'.join(('gw', self.get_gw()))
        cf.append(s)
        try:
            s = ';'.join(('hv_root_pubkey', self.get_pubkey()))
            cf.append(s)
        except ex.excError:
            pass
        for resource in self.r.svc.get_resources("ip"):
            s = ';'.join((resource.rid, resource.ipdev, resource.addr, resource.mask))
            cf.append(s)
        cf.append('')
        return '\n'.join(cf)

    def setup_cfdisk(self):
        config = self.get_config()
        block = len(config)//512 + 1
        cfdisk = os.path.join(rcEnv.paths.pathtmp, self.r.svc.name+'.cfdisk')
        try:
            with open(cfdisk, 'w') as f:
                f.write(config)
                f.seek(block*512)
                f.write('\0')
        except:
            self.r.log.error("failed to create config disk")
            raise ex.excError
        self.virtinst_cfdisk = ["--disk", "path=%s,device=floppy"%cfdisk]
        self.r.log.info("created config disk with content;\n%s", config)

    def provisioner(self):
        self.setup_snap()
        self.setup_cfdisk()
        self.setup_kvm()
        self.setup_ips()
        self.r.log.info("provisioned")
        return True
