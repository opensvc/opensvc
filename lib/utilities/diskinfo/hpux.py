import os

from utilities.proc import justcall, which
from .diskinfo import BaseDiskInfo

class DiskInfo(BaseDiskInfo):
    legacy_size_cache = {}
    legacy_wwid_cache = {}

    def load_cache(self):
        self.load_aliases()

        self.h = {}
        cmd = ["scsimgr", "-p", "get_attr", "all_lun", "-a", "wwid", "-a", "device_file", "-a", "vid", "-a", "pid", "-a", "capacity"]
        out, err, ret = justcall(cmd)
        for e in out.split('\n'):
            if len(e) == 0:
                continue
            (wwid, dev, vid, pid, size) = e.split(':')
            wwid = wwid.replace('0x', '')
            if len(size) != 0:
                size = int(size)/2048
            else:
                size = 0
            vid = vid.strip('" ')
            pid = pid.strip('" ')
            if dev in self.aliases:
                aliases = self.aliases[dev]
            else:
                aliases = [dev]
            for alias in aliases:
                self.h[alias] = dict(wwid=wwid, vid=vid, pid=pid, size=size)

    def load_ioscan(self, refresh=False):
        if not refresh:
            try:
                return getattr(self, "ioscan")
            except AttributeError:
                pass
        cmd = ['/usr/sbin/ioscan', '-FunNC', 'disk']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        self.ioscan = []
        """
        virtbus:wsio:T:T:F:1:13:10:disk:esdisk:64000/0xfa00/0xa:0 0 4 50 0 0 0 0 51 248 164 14 250 83 253 237 :18:root.ext_virtroot.esvroot.esdisk:esdisk:CLAIMED:DEVICE:EMC     SYMMETRIX:-1:online
                      /dev/disk/disk17            /dev/disk/disk17_p3         /dev/rdisk/disk17_p1
                      /dev/disk/disk17_p1         /dev/pt/x64lmwbieb9_system  /dev/rdisk/disk17_p2
                      /dev/disk/disk17_p2         /dev/rdisk/disk17           /dev/rdisk/disk17_p3
        """
        for line in out.split('\n'):
            if not line.startswith(' ') and not line.startswith('\t') and len(line) > 0:
                l = line.split(":")
                blk_major = l[5]
                raw_major = l[6]
                index = l[7]
                vendor = l[17]
                # mark ready for insertion as soon as we get a devname
                devname = None
            elif devname is None:
                devname = line.split()[0]
                self.ioscan.append({
                  'devname': devname,
                  'dev': ':'.join((blk_major, index)),
                  'rdev': ':'.join((raw_major, index)),
                  'vendor': vendor,
                })
        return self.ioscan

    def load_aliases(self):
        self.aliases = {}
        cmd = ['/usr/sbin/ioscan', '-FunNC', 'disk']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        l = []
        for line in out.split('\n')+[':']:
            if ':' in line:
                if len(l) > 0:
                    for name in l:
                         self.aliases[name] = l
                l = []
                continue
            for w in line.split():
                l.append(w)

    def dev2char(self, dev):
        dev = dev.replace("/dev/disk/", "/dev/rdisk/")
        dev = dev.replace("/dev/dsk/", "/dev/rdsk/")
        return dev

    def scan(self, dev):
        cmd = ["scsimgr", "-p", "get_attr", "-D", self.dev2char(dev), "-a", "wwid", "-a", "device_file", "-a", "vid", "-a", "pid", "-a", "capacity"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.h[dev] = dict(wwid="", vid="", pid="", size=0)
            return
        (wwid, foo, vid, pid, size) = out.split(':')
        wwid = wwid.replace('0x', '')
        if len(size) != 0:
            size = int(size)/2048
        else:
            size = 0
        vid = vid.strip('" ')
        pid = pid.strip('" ')
        self.h[dev] = dict(wwid=wwid, vid=vid, pid=pid, size=size)

    def get(self, dev, type):
        if not self.h:
            self.load_cache()
        if dev not in self.h:
            self.scan(dev)
        return self.h[dev][type]

    def disk_id(self, dev):
        id = self.get(dev, 'wwid')
        if len(id) == 0:
            id = self.get_legacy_wwid(dev)
        return id

    def disk_vendor(self, dev):
        return self.get(dev, 'vid')

    def disk_model(self, dev):
        return self.get(dev, 'pid')

    def disk_size(self, dev):
        size = self.get(dev, 'size')
        if size == 0:
            size = self.get_legacy_size(dev)
        if size is None or size == "":
            # broken disk
            size = 0
        return size

    def diskinfo_str(self, info):
        info['size'] = self.disk_size(info['devname'])
        info['hbtl'] = "#:#:#:#"
        return self.print_diskinfo_fmt%(
          info['hbtl'],
          os.path.basename(info['devname']),
          info['size'],
          info['dev'],
          info['vendor'],
          '',
        )

    def print_diskinfo(self, info):
        print(self.diskinfo_str(info))

    def scanscsi(self, hba=None, target=None, lun=None, log=None):
        ioscan_before = self.load_ioscan()
        disks_before = map(lambda x: x['devname'], ioscan_before)

        cmd = ['/usr/sbin/ioscan', '-fnC', 'disk']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return

        ioscan_after = self.load_ioscan(refresh=True)
        disks_after = map(lambda x: x['devname'], ioscan_after)
        new_disks = set(disks_after) - set(disks_before)

        if log:
            log.info(self.diskinfo_header())
        for info in ioscan_after:
            if info['devname'] not in new_disks:
                continue
            if log:
                log.info(self.diskinfo_str(info))

        return 0

    def get_legacy_wwid(self, devpath):
        if devpath in self.legacy_wwid_cache:
            self.legacy_wwid_cache[devpath]
        if which("autopath"):
            wwid = self.get_autopath_wwid(devpath)
            self.legacy_wwid_cache[devpath] = wwid
            return wwid
        return ""

    def get_autopath_wwid(self, devpath):
        cmd = ["autopath", "display", devpath]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return ""
        for line in out.split("\n"):
            if "Lun WWN" in line:
                return line.split(": ")[-1].replace("-","").lower()
        return ""

    def get_legacy_size(self, devpath):
        """ return devpath size in megabytes
        """
        if devpath in self.legacy_size_cache:
            return self.legacy_size_cache[devpath]
        if not which("diskinfo"):
            return 0
        cmd = ["diskinfo", "-b", devpath.replace("dsk", "rdsk").replace("disk", "rdisk")]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return 0
        size = int(out.strip())/1024
        self.legacy_size_cache[devpath] = size
        return size

