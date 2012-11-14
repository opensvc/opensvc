from provisioning import Provisioning
from rcGlobalEnv import rcEnv
from rcUtilities import which
import os
import rcExceptions as ex

class ProvisioningKvm(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

        self.section = r.svc.config.defaults()

        if 'snapof' in self.section:
            self.snapof = self.section['snapof']
        else:
            self.snapof = None

        if 'snap' in self.section:
            self.snap = self.section['snap']
        else:
            self.snap = None

        if 'virtinst' in self.section:
            self.virtinst = self.section['virtinst']
        else:
            self.virtinst = None

    def check_kvm(self):
        if os.path.exists(self.r.cf):
            return True
        return False

    def setup_kvm(self):
        if self.virtinst is None:
            self.r.log.error("the 'virtinst' parameter must be set")
            raise ex.excError
        ret, out, err = self.r.vcall(self.virtinst.split())
        if ret != 0:
            raise ex.excError

    def setup_ips(self):
        self.purge_known_hosts()
        for rs in self.r.svc.get_res_sets("ip"):
            for r in rs.resources:
                self.purge_known_hosts(r.addr)

    def purge_known_hosts(self, ip=None):
        if ip is None:
            cmd = ['ssh-keygen', '-R', self.r.svc.svcname]
        else:
            cmd = ['ssh-keygen', '-R', ip]
        ret, out, err = self.r.vcall(cmd, err_to_info=True)

    def setup_snap(self):
        if self.snap is None:
            self.r.log.error("the 'snap' parameter must be set")
            raise ex.excError
        if self.snapof is None:
            self.r.log.error("the 'snapof' parameter must be set")
            raise ex.excError
        if not which('btrfs'):
            self.r.log.error("'btrfs' command not found")
            raise ex.excError
 
        cmd = ['btrfs', 'subvolume', 'snapshot', self.snapof, self.snap]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def get_pubkey(self):
        p = os.path.join(os.sep, 'root', '.ssh', 'id_dsa.pub')
        try:
            with open(p) as f:
                pub = f.read(8000)
        except:
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
        for rs in self.r.svc.get_res_sets("ip"):
            for r in rs.resources:
                s = ';'.join((r.rid, r.ipDev, r.addr, r.mask))
                cf.append(s)
        cf.append('')
        return '\n'.join(cf)

    def setup_cfdisk(self):
        config = self.get_config()
        block = len(config)//512 + 1
        cfdisk = os.path.join(rcEnv.pathtmp, self.r.svc.svcname+'.cfdisk')
        try:
            with open(cfdisk, 'w') as f:
                f.write(config)
                f.seek(block*512)
                f.write('\0')
        except:
            self.r.log.error("failed to create config disk")
            raise ex.excError
        self.virtinst += " --disk path=%s,device=floppy"%cfdisk
        self.r.log.info("created config disk with content;\n%s", config)

    def provisioner(self):
        self.setup_snap()
        self.setup_cfdisk()
        self.setup_kvm()
        self.setup_ips()

        self.r.log.info("provisioned")
        return True
