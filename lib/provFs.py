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
        cmd = self.info + [self.mkfs_dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        self.r.log.info("%s is not formatted"%self.mkfs_dev)
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

        # /dev/mapper/$vg-$lv and /dev/$vg/$lv creation is delayed ... refresh
        self.r.vcall(["dmsetup", "mknodes"])
        mapname = "%s-%s"%(self.section['vg'].replace('-','--'),
                           dev.replace('-','--'))
        self.dev = '/dev/mapper/'+mapname
        import time
        for i in range(3, 0, -1):
            if os.path.exists(self.dev):
                break
            if i != 0:
                time.sleep(1)
        if i == 0:
            self.r.log.error("timed out waiting for %s to appear"%self.dev)
            raise ex.excError
            
    def provision_dev_hpux(self):
        if not which('vgdisplay'):
            self.r.log.error("vgdisplay command not found")
            raise ex.excError
        cmd = ['vgdisplay', self.section['vg']]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.r.log.error("volume group %s does not exist"%self.section['vg'])
            raise ex.excError
        dev = os.path.basename(self.section['dev'])
        if which('lvcreate'):
            # create the logical volume
            cmd = ['lvcreate', '-n', dev, '-L', str(self.section['size'])+'M', self.section['vg']]
            ret, out, err = self.r.vcall(cmd)
            if ret != 0:
                raise ex.excError
        else:
            self.r.log.error("lvcreate command not found")
            raise ex.excError


    def provision_dev(self):
        if 'vg' not in self.section:
            return
        if 'size' not in self.section:
            return
        vg = self.section['vg']
        if rcEnv.sysname == 'Linux':
            self.provision_dev_linux()
        elif rcEnv.sysname == 'HP-UX':
            self.provision_dev_hpux()
           
    def provisioner(self):
        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
            self.r.log.info("%s mount point created"%self.mnt)

        if not os.path.exists(self.dev):
            self.provision_dev()

        self.mkfs_dev = self.dev
        if rcEnv.sysname == 'HP-UX':
            l = self.dev.split('/')
            l[-1] = 'r'+l[-1]
            self.mkfs_dev = '/'.join(l)
            if not os.path.exists(self.mkfs_dev):
               self.r.log.error("%s raw device does not exists"%self.mkfs_dev)
               return

        if not self.check_fs():
            cmd = self.mkfs + [self.mkfs_dev]
            (ret, out, err) = self.r.vcall(cmd)
            if ret != 0:
                self.r.log.error('Failed to format %s'%self.mkfs_dev)
                raise ex.excError

        self.r.log.info("provisioned")
        self.r.start()
        return True
