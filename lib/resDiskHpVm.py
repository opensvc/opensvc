import re
import os
import rcExceptions as ex
import rcStatus
resVg = __import__("resDiskVgHP-UX")
from subprocess import *
from rcUtilities import qcall
from rcGlobalEnv import rcEnv
from subprocess import *

class Disk(resVg.Disk):
    def __init__(self,
                 rid=None,
                 name=None,
                 container_name=None,
                 type=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        self.label = "vmdg "+str(name)
        self.container_name = container_name
        resVg.Disk.__init__(self,
                          rid=rid,
                          name=name,
                          type='disk.vg',
                          monitor=monitor,
                          restart=restart,
                          optional=optional,
                          disabled=disabled,
                          tags=tags,
                          subset=subset)

    def has_it(self):
        return True

    def is_up(self):
        return True

    def _status(self, verbose=False):
        return rcStatus.NA

    def do_start(self):
        self.do_mksf()

    def do_stop(self):
        pass

    def files_to_sync(self):
        return [self.sharefile_name(), self.mkfsfile_name()]

    def postsync(self):
        s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
        if s['overall'].status != rcStatus.UP:
            self.do_mksf()
            self.do_share()

    def presync(self):
        s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
        if self.svc.options.force or s['overall'].status == rcStatus.UP:
            self.write_mksf()
            self.write_share()

    def sharefile_name(self):
        return os.path.join(rcEnv.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.share')

    def get_devs(self):
        cmd = ['/opt/hpvm/bin/hpvmdevmgmt', '-l', 'all']
        (ret, buff, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        if len(buff) == 0:
            return []
        a = {}
        for line in buff.split('\n'):
            if len(line) == 0:
                continue
            if "DEVTYPE=FILE" not in line and "DEVTYPE=DISK" not in line:
                continue
            if "SHARE=YES" in line:
                share = "YES"
            else:
                share = "NO"
            devs = line.split(":")[0]
            for dev in devs.split(","):
                a[dev] = {'share': share}
        return a

    def write_share(self):
        devs = self.get_devs()
        disklist = self.disklist()
        with open(self.sharefile_name(), 'w') as f:
            for dev in devs:
                if dev not in disklist:
                    continue
                f.write("%s:%s\n"%(dev, devs[dev]['share']))

    def do_share(self):
        if not os.path.exists(self.sharefile_name()):
            return
        devs = self.get_devs()
        errors = 0
        with open(self.sharefile_name(), 'r') as f:
            for line in f.readlines():
                l = line.split(':')
                if len(l) != 2:
                    continue
                dev = l[0]
                share = l[1].strip()
                if len(dev) == 0:
                    continue
                if not os.path.exists(dev):
                    continue
                if dev not in devs:
                    cmd = ['/opt/hpvm/bin/hpvmdevmgmt', '-a', 'gdev:'+dev]
                    (ret, out, err) = self.vcall(cmd)
                    if ret != 0:
                        self.log.error("error adding device %s hpvm device table"%dev)
                        raise ex.excError
                if dev in devs and share == devs[dev]['share']:
                    self.log.debug("skip set sharing of %s: already set to %s"%(dev, devs[dev]['share']))
                    continue
                cmd = ['/opt/hpvm/bin/hpvmdevmgmt', '-m', 'gdev:'+dev+':attr:SHARE='+share]
                (ret, buff, err) = self.vcall(cmd)
                if ret != 0:
                    self.log.error("error setting the shared attribute for %s"%dev)
                    errors += 1
                    continue
        if errors > 0:
            raise ex.excError

    def disklist(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-d', '-P', self.container_name]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = p.communicate()
        if p.returncode != 0:
            raise ex.excError

        for line in buff[0].split('\n'):
            l = line.split(':')
            if len(l) < 5:
                continue
            if l[3] != 'disk':
                continue
            self.disks |= set([l[4]])
        return self.disks
