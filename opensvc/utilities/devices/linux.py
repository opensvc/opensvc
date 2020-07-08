import json
import os
import re
import glob
import time

import core.exceptions as ex
from env import Env
from core.capabilities import capabilities
from utilities.cache import cache
from utilities.proc import justcall, call, qcall

label_to_dev_cache = {}


def udevadm_settle():
    if "node.x.udevadm" not in capabilities:
        return
    cmd = ["udevadm", "settle"]
    justcall(cmd)

def udevadm_query_symlink(dev):
    cmd = ["udevadm", "info", "-q", "symlink", dev]
    out, err, ret = justcall(cmd)
    if ret != 0:
        return []
    return ["/dev/"+dev for dev in out.split() if dev]

def dev_to_paths(dev, log=None):
    dev = os.path.realpath(dev)
    if dev.startswith("/dev/sd"):
        return [dev]
    if dev.startswith("/dev/vx"):
        from utilities.subsystems.veritas import vx_dev_to_paths
        return vx_dev_to_paths(dev)
    if not dev.startswith("/dev/dm-"):
        return []
    name = os.path.basename(dev)
    cmd = ["dmsetup", "table", "-j", str(major("device-mapper")), "-m", dev[8:]]
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.Error(err)
    if "multipath" not in out:
        return []
    paths = ["/dev/"+os.path.basename(_name) for _name in glob.glob("/sys/block/%s/slaves/*" % name)]
    return paths

def dev_set_rw(dev, log=None):
    cmd = ["blockdev", "--setrw", dev]
    if log:
        log.info(" ".join(cmd))
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.Error(err)

def dev_is_ro_ioctl(dev):
    cmd = ["blockdev", "--getro", dev]
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.Error(err)
    if out.strip() == "1":
        return True
    return False

def dev_is_ro_sysfs(dev):
    dev = dev.replace('/dev/', '')
    sysdev = "/sys/block/%s/ro"%dev
    with open(sysdev, 'r') as s:
        buff = s.read()
    if buff.strip() == "1":
        return True
    return False

def dev_is_ro(dev):
    try:
        return dev_is_ro_ioctl(dev)
    except:
        return dev_is_ro_sysfs(dev)

def need_rescan(dev):
    try:
        fd = os.open(dev, os.O_NONBLOCK|os.O_RDWR)
        os.close(fd)
        return False
    except (OSError, IOError):
        return True

def dev_rescan(dev, log=None):
    dev = dev.replace('/dev/', '')
    sysdev = "/sys/block/%s/device/rescan" % dev
    if log:
        log.info("echo 1>%s"%sysdev)
    with open(sysdev, 'w') as s:
        s.write("1")

def dev_delete(dev, log=None):
    dev = dev.replace('/dev/', '')
    sysdev = "/sys/block/%s/device/delete" % dev
    if log:
        log.info("echo 1>%s"%sysdev)
    with open(sysdev, 'w') as s:
        s.write("1")

def refresh_multipath(dev, log=None):
    cmd = [Env.syspaths.multipath, "-v0", "-r", dev]
    (ret, out, err) = call(cmd, info=True, outlog=True, log=log)
    if ret != 0:
        raise ex.Error

def multipath_flush(dev, log=None):
    """
    Settle udev before running a "multipath -f <dev>" to avoid
    the "in use" error.
    """
    udevadm_settle()
    cmd = [Env.syspaths.multipath, "-f", dev]
    ret, out, err = call(cmd, info=True, outlog=True, log=log)
    if ret != 0:
        raise ex.Error

def dev_ready(dev, log=None):
    cmd = ['sg_turs', dev]
    (ret, out, err) = call(cmd, info=True, outlog=True, log=log)
    if ret != 0:
        return False
    return True

def wait_for_dev_ready(dev, log=None):
    delay = 1
    timeout = 5
    for i in range(timeout//delay):
        if dev_ready(dev, log=log):
            return
        if i == 0:
            if log:
                log.info("waiting for device %s to become ready (max %i secs)"%(dev,timeout))
        time.sleep(delay)
    if log:
        log.error("timed out waiting for device %s to become ready (max %i secs)"%(dev,timeout))
    raise ex.Error

def promote_dev_rw(dev, log=None):
    count = 0
    for _dev in dev_to_paths(dev, log=log):
       changed = False
       if dev_is_ro(_dev):
           try:
               dev_set_rw(_dev, log=log)
               changed = True
           except:
               pass
       if need_rescan(_dev):
           dev_rescan(_dev, log=log)
           changed = True
       if changed:
           count += 1
           wait_for_dev_ready(_dev, log=log)
    if dev_is_ro(dev):
        try:
            dev_set_rw(dev, log=log)
        except ex.Error:
            pass
    if count > 0:
        try:
            refresh_multipath(dev, log=log)
        except ex.Error:
            pass

def loop_is_deleted(dev):
    if "node.x.losetup" not in capabilities:
        raise ex.Error("losetup must be installed")
    out, err, ret = justcall([Env.syspaths.losetup, dev])
    if "(deleted)" in out:
        return True
    return False

def label_to_dev(label, tree=None):
    """
       blkid can return a device slave of a drbd, as drbd is
       transparent wrt to signature detection. Detect this case
       and return the holding device. Otherwise return None.
    """
    if label in label_to_dev_cache:
        return label_to_dev_cache[label]

    if "node.x.blkid" not in capabilities:
        return
    out, err, ret = justcall([Env.syspaths.blkid, "-t", label])
    if ret != 0:
        return
    devps = []
    for line in out.split("\n"):
        if len(line) == 0:
            continue
        devp = line.split(":")[0]
        if devp.startswith("/dev/loop") and loop_is_deleted(devp):
            continue
        devps.append(devp)

    if len(devps) == 0:
        return
    elif len(devps) == 1:
        return devps[0]

    if tree is None:
        from utilities.devtree import DevTree
        tree = DevTree()
        tree.load()
    devs = tree.get_devs_by_devpaths(devps)
    for dev in devs:
        parent_devps = set()
        for p in dev.parents:
            d = tree.get_dev(p.parent)
            if d is None:
                continue
            parent_devps |= set(d.devpath)
        inter = set(devps) & parent_devps
        if len(inter) > 0:
            devp = "/dev/"+dev.devname
            label_to_dev_cache[label] = devp
            return devp

    raise ex.Error("multiple devs match the label: %s" % ", ".join(devps))

def major(driver):
    path = os.path.join(os.path.sep, 'proc', 'devices')
    try:
        f = open(path)
    except:
        return -1
    for line in f.readlines():
        words = line.split()
        if len(words) == 2 and words[1] == driver:
            f.close()
            return int(words[0])
    f.close()
    return -1

def get_blockdev_sd_slaves(syspath):
    slaves = set()
    if not os.path.exists(syspath):
        return slaves
    for s in os.listdir(syspath):
        if re.match('^sd[a-z]*', s) is not None:
            slaves.add('/dev/' + s)
            continue
        deeper = os.path.join(syspath, s, 'slaves')
        if os.path.isdir(deeper):
            slaves |= get_blockdev_sd_slaves(deeper)
    return slaves

def lv_exists(self, device):
    if qcall([Env.syspaths.lvs, device]) == 0:
        return True
    return False

def lv_info(self, device):
    cmd = [
        Env.syspaths.lvs,
        '-o', 'vg_name,lv_name,lv_size',
        '--noheadings', '--units', 'm',
        device
    ]
    ret, buff, err = self.call(cmd)
    if ret != 0:
        return (None, None, None)
    info = buff.split()
    if 'M' in info[2]:
        lv_size = float(info[2].split('M')[0])
    elif 'm' in info[2]:
        lv_size = float(info[2].split('m')[0])
    else:
        self.log.error("%s output does not have the expected unit (m or M)"%' '.join(cmd))
        ex.Error
    return (info[0], info[1], lv_size)

def get_partition_parent(dev):
    syspath = '/sys/block/*/' + os.path.basename(dev)
    l = glob.glob(syspath)
    if len(l) == 1:
        return '/dev/'+l[0].split('/')[3]
    return None

def devs_to_disks(self, devs=None):
    """ If PV is a device map, replace by its sysfs name (dm-*)
        If device map has slaves, replace by its slaves
    """
    devs = devs or set()
    disks = set()
    dm_major = major('device-mapper')
    try: md_major = major('md')
    except: md_major = 0
    try: lo_major = major('loop')
    except: lo_major = 0
    for dev in devs:
        try:
            statinfo = os.stat(dev)
        except:
            self.log.debug("can not stat %s" % dev)
            continue
        if md_major != 0 and os.major(statinfo.st_rdev) == md_major:
            md = dev.replace("/dev/", "")
            syspath = '/sys/block/' + md + '/slaves'
            disks |= get_blockdev_sd_slaves(syspath)
        elif os.major(statinfo.st_rdev) == dm_major:
            dm = 'dm-' + str(os.minor(statinfo.st_rdev))
            syspath = '/sys/block/' + dm + '/slaves'
            disks |= get_blockdev_sd_slaves(syspath)
        elif lo_major != 0 and os.major(statinfo.st_rdev) == lo_major:
            self.log.debug("skip loop device %s from disk list"%dev)
            pass
        else:
            parent = get_partition_parent(dev)
            if parent is not None:
                disks.add(parent)
            else:
                disks.add(dev)
    _disks = list(disks)
    for i, disk in enumerate(_disks):
        _disks[i] = re.sub("^(/dev/[vhs]d[a-z]*)[0-9]*$", r"\1", disk)
    return disks

@cache("losetup.json")
def losetup_data():
    cmd = ["losetup", "-J"]
    out, err, ret = justcall(cmd)
    try:
        return json.loads(out)["loopdevices"]
    except ValueError:
        return

def file_to_loop(f):
    """
    Given a file path, returns the loop device associated. For example,
    /path/to/file => /dev/loop0
    """
    data = losetup_data()
    if data:
        return [_data["name"] for _data in data if _data["back-file"] == f]

    out, err, ret = justcall([Env.syspaths.losetup, '-j', f])
    if len(out) == 0:
        return []

    # It's possible multiple loopdev are associated with the same file
    devs = []
    for line in out.split('\n'):
        l = line.split(':')
        if len(l) == 0:
            continue
        if len(l[0]) == 0:
            continue
        if not os.path.exists(l[0]):
            continue
        devs.append(l[0])
    return devs

def loop_to_file(f):
    """
    Given a loop dev, returns the loop file associated. For example,
    /dev/loop0 => /path/to/file
    """
    data = losetup_data()
    if data:
        for _data in data:
            if _data["name"] == f:
                return _data["back-file"]
        return

    out, err, ret = justcall([Env.syspaths.losetup, f])
    if len(out) == 0:
        return

    for line in out.split('\n'):
        l = line.split('(')
        if len(l) == 0:
            continue
        fpath = l[-1].rstrip(")")
        if len(fpath) == 0:
            continue
        if not os.path.exists(fpath):
            continue
        return fpath

