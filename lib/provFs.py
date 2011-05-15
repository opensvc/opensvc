from provisioning import Provisioning
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv
import os
import rcExceptions as ex

class ProvisioningFs(Provisioning):
    # required from children:
    #   mkfs = ['mkfs.ext4', '-F']
    #   info = ['tune2fs', '-l']
    def __init__(self, r):
        Provisioning.__init__(self, r)
        self.section = dict(r.svc.config.items(r.rid))
        self.dev = self.section['dev']
        self.mnt = self.section['mnt']

    def check_fs(self):
        cmd = self.info + [self.dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        self.r.log.info("%s is not formatted"%self.dev)
        return False

    def provision_dev_linux(self):
        if not which('vgdisplay'):
            self.r.log.error("vgdisplay command not found")
            raise ex.excError
        cmd = ['vgdisplay', self.section['vg']]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.r.log.error("volume group %s does not exist"%self.section['vg'])
            raise ex.excError
        if self.dev.startswith('/dev/mapper/'):
            dev = self.dev.replace('/dev/mapper/').replace(self.section['vg'].replace('-', '--')+'-', '')
            dev = dev.replace('--', '-')
        elif self.section['vg'] in self.dev:
            dev = os.path.basename(self.dev)
        else:
            self.r.log.error("malformat dev %s"%self.dev)
            raise ex.excError
        if which('lvcreate'):
            # create the logical volume
            cmd = ['lvcreate', '-n', dev, '-L', str(self.section['size'])+'M', self.section['vg']]
            ret, out, err = self.r.vcall(cmd)
            if ret != 0:
                raise ex.excError
        else:
            self.r.log.error("lvcreate command not found")
            raise ex.excError

        # /dev/mapper/$vg-$lv and /dev/$vg/$lv creation is delayed ... use /dev/dm-$minor
        mapname = "%s-%s"%(self.section['vg'].replace('-','--'),
                           dev.replace('-','--'))
        cmd = ['dmsetup', 'info', mapname]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.r.log.error("failed to find logical volume device mapper info")
            raise ex.excError
        l = out.split('\n')
        minor = None
        for line in l:
            if line.startswith('Major'):
                minor = line.split()[-1]
        if minor is None:
            self.r.log.error("failed to find logical volume device mapper info")
            raise ex.excError
        self.dev = '/dev/dm-'+minor
        import time
        for i in range(3, 0, -1):
            if os.path.exists(self.dev):
                break
            if i != 0:
                time.sleep(1)
        if i == 0:
            self.r.log.error("timed out waiting for %s to appear"%self.dev)
            raise ex.excError
            

    def provision_dev(self):
        if 'vg' not in self.section:
            return
        if 'size' not in self.section:
            return
        vg = self.section['vg']
        if rcEnv.sysname == 'Linux':
            self.provision_dev_linux()
           
    def provisioner(self):
        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
            self.r.log.info("%s mount point created"%self.mnt)

        if not os.path.exists(self.dev):
            self.provision_dev()

        if not self.check_fs():
            cmd = self.mkfs + [self.dev]
            (ret, out, err) = self.r.vcall(cmd)
            if ret != 0:
                self.r.log.error('Failed to format %s'%self.dev)
                raise ex.excError

        self.r.log.info("provisioned")
        self.r.start()
        return True
