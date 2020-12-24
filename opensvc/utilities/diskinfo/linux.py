from __future__ import print_function

import glob
import json
import math
import os
import re
import sys
import time

import core.exceptions as ex
import utilities.devtree.veritas
import utilities.devices.linux
from .diskinfo import BaseDiskInfo

from env import Env
from utilities.lazy import lazy
from utilities.proc import justcall, which

class DiskInfo(BaseDiskInfo):
    disk_ids = {}

    def __init__(self, deferred=False):
        pass

    def prefix_local(self, id):
        return '.'.join((Env.nodename, id))

    def disk_id(self, dev):
        if 'cciss' in dev:
            id = self.cciss_id(dev)
        elif dev.startswith('/dev/disk/by-id/wwn-0x'):
            id = dev.replace('/dev/disk/by-id/wwn-0x', '')
        elif dev.startswith('/dev/disk/by-id/scsi-2'):
            id = dev.replace('/dev/disk/by-id/scsi-2', '')
        elif dev.startswith('/dev/disk/by-id/scsi-3'):
            id = dev.replace('/dev/disk/by-id/scsi-3', '')
        elif dev.startswith('/dev/mapper/3'):
            id = dev.replace('/dev/mapper/3', '')
        elif dev.startswith('/dev/mapper/2'):
            id = dev.replace('/dev/mapper/2', '')
        elif "dmp/" in dev:
            id = utilities.devtree.veritas.DevTreeVeritas().vx_inq(dev)
        elif "Google_PersistentDisk_" in dev or "google-" in dev:
            id = self.gce_disk_id(dev)
        else:
            id = self.scsi_id(dev)
        if len(id) == 0:
            return self.prefix_local(dev.replace('/dev/','').replace('/','!'))
        return id

    @lazy
    def gce_instance_data(self):
        cmd = ["gcloud", "compute", "instances", "describe", "-q", "--format", "json", Env.nodename]
        out, err, ret = justcall(cmd)
        return json.loads(out)

    def gce_disk_id(self, dev):
        if "Google_PersistentDisk_" in dev:
            devname = dev.split("Google_PersistentDisk_")[-1]
        else:
            devname = dev.split("google-")[-1]
        for disk in self.gce_instance_data["disks"]:
            if disk["deviceName"] != devname:
                continue
            i = disk["source"].index("/project")
            return str(disk["source"][i:].replace("/projects", "").replace("/zones", "").replace("/disks", ""))

    def cciss_id(self, dev):
        if dev in self.disk_ids:
            return self.disk_ids[dev]
        if which('cciss_id'):
            cciss_id = 'cciss_id'
        else:
            return ""
        cmd = [cciss_id, dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            id = out.split('\n')[0]
            if id.startswith('3'):
                id = id[1:]
            else:
                id = self.prefix_local(id)
            self.disk_ids[dev] = id
            return id
        return ""

    def mpath_id(self, dev):
        self.load_mpath()
        if dev not in self.mpath_h:
            return None
        return self.mpath_h[dev]

    def load_mpath_native(self):
        cmd = [Env.syspaths.multipath, '-l']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        lines = out.split('\n')
        if len(lines) == 0:
            return
        self.mpath_h = {}
        regex = re.compile('[(]*[0-9a-f]*[)]*')
        for line in lines:
            if len(line) > 0 and \
               line[0] not in (' ', '\\', '[', '`', '|'):
                l = line.split()
                if l[0].startswith("size="):
                    continue
                wwid = None
                for w in l:
                    w = w.strip("()")
                    if len(w) not in [17, 33]:
                        continue
                    if regex.match(w) is None:
                        continue
                    if w[0] in ("2,", "3", "5"):
                        wwid = w[1:]
            elif " sd" in line:
                l = line.split()
                for i, w in enumerate(l):
                    if w.startswith('sd'):
                        dev = "/dev/"+w
                        self.mpath_h[dev] = wwid

    def load_mpath(self):
        if hasattr(self, "mpath_h"):
            return self.mpath_h
        self.mpath_h = {}
        if which(Env.syspaths.multipath):
            self.load_mpath_native()
        return self.mpath_h

    def scsi_id(self, dev):
        s = self._scsi_id(dev, ["-p", "0x83"])
        if len(s) == 0:
            s = self._scsi_id(dev, ["-p", "pre-spc3-83"])
        return s

    def _scsi_id(self, dev, args=None):
        if args is None:
            args = []
        wwid = self.mpath_id(dev)
        if wwid is not None:
            return wwid
        if dev in self.disk_ids:
            return self.disk_ids[dev]
        if which('scsi_id'):
            scsi_id = 'scsi_id'
        elif which('/lib/udev/scsi_id'):
            scsi_id = '/lib/udev/scsi_id'
        else:
            return ""
        cmd = [scsi_id, '-g', '-u'] + args + ['-d', dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            id = out.split('\n')[0]
            if id.startswith('3') or id.startswith('2') or id.startswith('5'):
                id = id[1:]
            else:
                id = self.prefix_local(id)
            self.disk_ids[dev] = id
            return id
        sdev = dev.replace("/dev/", "/block/")
        cmd = [scsi_id, '-g', '-u'] + args + ['-s', sdev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            id = out.split('\n')[0]
            if id.startswith('3') or id.startswith('2') or id.startswith('5'):
                id = id[1:]
            else:
                id = self.prefix_local(id)
            self.disk_ids[dev] = id
            return id
        return ""

    def devpath_to_sysname(self, devpath):
        devpath = os.path.realpath(devpath)
        return os.path.basename(devpath)

    def disk_vendor(self, dev):
        if 'cciss' in dev:
            return 'HP'
        s = ''
        dev = self.devpath_to_sysname(dev)
        if dev.startswith("sd"):
            dev = re.sub("[0-9]+$", "", dev)
        path = '/sys/block/%s/device/vendor' % dev
        if not os.path.exists(path):
            l = glob.glob("/sys/block/%s/slaves/*/device/vendor" % dev)
            if len(l) > 0:
                path = l[0]
        if not os.path.exists(path):
            return ""
        with open(path, 'r') as f:
            s = f.read()
            f.close()
        if '6900' in s:
            s = 'Red Hat'
        return s.strip()

    def disk_model(self, dev):
        if 'cciss' in dev:
            return 'VOLUME'
        s = ''
        vendor = self.disk_vendor(dev)
        dev = self.devpath_to_sysname(dev)
        if dev.startswith("sd"):
            dev = re.sub("[0-9]+$", "", dev)
        path = '/sys/block/%s/device/model' % dev
        if not os.path.exists(path):
            l = glob.glob("/sys/block/%s/slaves/*/device/model" % dev)
            if len(l) > 0:
                path = l[0]
        if not os.path.exists(path):
            if 'Red Hat' in vendor:
                return 'VirtIO'
            else:
                return ""
        with open(path, 'r') as f:
            s = f.read()
            f.close()
        return s.strip()

    def disk_size(self, dev):
        size = 0
        if '/dev/mapper/' in dev:
            try:
                statinfo = os.stat(dev)
            except:
                raise Exception("can not stat %s" % dev)
            dm = 'dm-' + str(os.minor(statinfo.st_rdev))
            path = '/sys/block/' + dm + '/size'
            if not os.path.exists(path):
                return 0
        else:
            path = dev.replace('/dev/', '/sys/block/')+'/size'
            if not os.path.exists(path):
                cmd = ['blockdev', '--getsize', dev]
                out, err, ret = justcall(cmd)
                if ret != 0:
                    return 0
                return int(math.ceil(1.*int(out)/2048))

        with open(path, 'r') as f:
            size = f.read()
            f.close()
        return int(math.ceil(1.*int(size)/2048))

    def diskinfo_str(self, disk):
        name = os.path.basename(disk)
        info = {
          'dev': '',
          'size': 0,
          'device/vendor': '',
          'device/model': '',
        }
        for i in info:
            i_f = os.path.join(disk, i)
            if not os.path.exists(i_f):
                continue
            with open(i_f, 'r') as f:
                info[i] = f.read().strip()
        if '6900' in info['device/vendor']:
            info['device/vendor'] = 'Red Hat'
            if info['device/model'] == '':
                info['device/model'] = 'VirtIO'
        info['hbtl'] = os.path.basename(os.path.realpath(os.path.join(disk, "device")))
        return self.print_diskinfo_fmt%(
          info['hbtl'],
          name,
          int(float(info['size'])/2//1024),
          info['dev'],
          info['device/vendor'],
          info['device/model'],
        )

    def print_diskinfo(self, disk):
        print(self.diskinfo_str(disk))

    def hba_num(self, hba=None):
        if hba is None:
            return
        if hba.startswith("iqn"):
            for path in glob.glob("/sys/class/scsi_host/host*"):
                for _path in glob.glob(path+"/device/session*/iscsi_session/session*/initiatorname"):
                    with open(_path, "r") as f:
                        content = f.read().strip()
                    if content == hba:
                        return os.path.basename(path).replace("host", "")
        for path in glob.glob("/sys/class/fc_host/host*"):
            for _path in glob.glob(path+"/port_name"):
                with open(_path, "r") as f:
                    content = f.read().strip()
                if content == hba or "0x"+content == hba:
                    return os.path.basename(path).replace("host", "")

    def target_num(self, host_num, target=None):
        if target is None:
            return
        if target.startswith("iqn"):
            for path in glob.glob("/sys/class/scsi_host/host"+str(host_num)+"/device/session*"):
                for _path in glob.glob(path+"/iscsi_session/session1/targetname"):
                    with open(_path, "r") as f:
                        content = f.read().strip()
                    if content == target:
                        path = glob.glob(path+"/target*:*:*")[0]
                        return path.split(":")[-1]
        for path in glob.glob("/sys/class/fc_transport/target%s:*:*" % str(host_num)):
            for _path in glob.glob(path+"/port_name"):
                with open(_path, "r") as f:
                    content = f.read().strip()
                if content == target or "0x"+content == target:
                    return os.path.basename(path).split(":")[-1]

    def scanscsi(self, hba=None, target=None, lun=None, log=None):
        if not os.path.exists('/sys') or not os.path.ismount('/sys'):
            print("scanscsi is not supported without /sys mounted", file=sys.stderr)
            return 1

        disks_before = glob.glob('/sys/block/sd*')
        disks_before += glob.glob('/sys/block/vd*')

        hba_num = self.hba_num(hba)
        if hba_num is not None:
            hosts = glob.glob('/sys/class/scsi_host/host'+str(hba_num))
            target_num = self.target_num(hba_num, target)
        else:
            hosts = glob.glob('/sys/class/scsi_host/host*')
            target_num = None

        if target_num is None:
            target_num = '-'

        if lun is None:
            lun = '-'

        for host in hosts:
            scan_f = host+'/scan'
            if not os.path.exists(scan_f):
                continue
            if log:
                log.info("scan %s target%s lun%s", os.path.basename(host), target_num, lun)
            os.system('echo - ' + target_num + ' ' + lun + ' >' + scan_f)

        utilities.devices.linux.udevadm_settle()
        disks_after = glob.glob('/sys/block/sd*')
        disks_after += glob.glob('/sys/block/vd*')
        new_disks = set(disks_after) - set(disks_before)

        if len(new_disks) == 0:
            if log:
                log.info("scsi scan found no new disk")
            return 0

        if log:
            log.info(self.diskinfo_header())
        for disk in new_disks:
            if log:
                log.info(self.diskinfo_str(disk))
            self.wait_devpath("/dev/"+disk.replace("/sys/block/", ""))

        return 0

    def wait_devpath(self, dev, tmo=10):
        for retry in range(tmo):
            if os.path.exists(dev):
                return
            time.sleep(1)
        raise ex.Error("time out waiting for %s to appear" % dev)

if __name__ == "__main__":
    diskinfo = DiskInfo()
    disks = glob.glob('/sys/block/sd*')
    disks += glob.glob('/sys/block/vd*')
    diskinfo.print_diskinfo_header()
    for disk in disks:
         diskinfo.print_diskinfo(disk)
    #dev = '/dev/vda'
    #vendor = diskinfo.disk_vendor(dev)
    #model = diskinfo.disk_model(dev)
    #print("%s has vendor [%s] model [%s]" % (dev, vendor, model))
