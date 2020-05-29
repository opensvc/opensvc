import os

from subprocess import *

import core.exceptions as ex
import core.status
from core.objects.svcdict import KEYS
from ..vg.hpux import DiskVg
from .. import BASE_KEYWORDS

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "hpvm"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "container_name",
        "at": True,
        "required": True,
        "text": "The name of the container to map the disks in its configuration file to the resource."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


def driver_capabilities(node=None):
    from utilities.proc import which
    if which("/opt/hpvm/bin/hpvmdevmgmt"):
        return "disk.hpvm"
    return []


class DiskHpvm(DiskVg):
    def __init__(self, container_name=None, **kwargs):
        kwargs["name"] = "vmdg_%s" % container_name
        super(DiskHpvm, self).__init__(**kwargs)
        self.label = "vmdg %s" % self.name
        self.container_name = container_name

    def has_it(self):
        return True

    def is_up(self):
        return True

    def _status(self, verbose=False):
        return core.status.NA

    def do_start(self):
        self.do_mksf()

    def do_stop(self):
        pass

    def files_to_sync(self):
        return [self.sharefile_name(), self.mksffile_name()]

    def postsync(self):
        s = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))
        if s['overall'].status != core.status.UP:
            self.do_mksf()
            self.do_share()

    def presync(self):
        s = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))
        if self.svc.options.force or s['overall'].status == core.status.UP:
            self.write_mksf()
            self.write_share()

    def sharefile_name(self):
        return os.path.join(self.var_d, 'share')

    def get_devs(self):
        cmd = ['/opt/hpvm/bin/hpvmdevmgmt', '-l', 'all']
        (ret, buff, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error
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
        sub_devs = self.sub_devs()
        with open(self.sharefile_name(), 'w') as f:
            for dev in devs:
                if dev not in sub_devs:
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
                        raise ex.Error
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
            raise ex.Error

    def sub_devs(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-d', '-P', self.container_name]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = p.communicate()
        if p.returncode != 0:
            raise ex.Error

        devs = set()
        for line in buff[0].split('\n'):
            l = line.split(':')
            if len(l) < 5:
                continue
            if l[3] != 'disk':
                continue
            devs |= set([l[4]])
        return devs
