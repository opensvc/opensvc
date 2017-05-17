import os
import re
import glob
from rcUtilities import call, qcall, justcall, which

label_to_dev_cache = {}

def udevadm_settle():
    if not which("udevadm"):
        return
    cmd = ["udevadm", "settle"]
    justcall(cmd)

def dev_to_paths(dev, log=None):
    cmd = ['multipath', '-l', dev]
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.excError
    paths = []
    for line in out.split("\n"):
        l = line.split()
        if len(l) < 3:
            continue
        dev = l[2]
        if not dev.startswith("sd"):
            continue
        paths.append("/dev/"+dev)
    return paths

def dev_is_ro(dev):
    dev = dev.replace('/dev/', '')
    sysdev = "/sys/block/%s/ro"%dev
    with open(sysdev, 'r') as s:
        buff = s.read()
    if buff.strip() == "1":
        return True

def dev_rescan(dev, log=None):
    dev = dev.replace('/dev/', '')
    sysdev = "/sys/block/%s/device/rescan"%dev
    if log:
        log.info("echo 1>%s"%sysdev)
    with open(sysdev, 'w') as s:
        s.write("1")

def refresh_multipath(dev, log=None):
    cmd = ['multipath', '-v0', '-r', dev]
    (ret, out, err) = call(cmd, info=True, outlog=True, log=log)
    if ret != 0:
        raise ex.excError

def dev_ready(dev, log=None):
    cmd = ['sg_turs', dev]
    (ret, out, err) = call(cmd, info=True, outlog=True, log=log)
    if ret != 0:
        return False
    return True

def wait_for_dev_ready(dev, log=None):
    delay = 1
    timeout = 5
    for i in range(timeout/delay):
        if dev_ready(dev, log=log):
            return
        if i == 0:
            if log:
                log.info("waiting for device %s to become ready (max %i secs)"%(dev,timeout))
        time.sleep(delay)
    if log:
        log.error("timed out waiting for device %s to become ready (max %i secs)"%(dev,timeout))
    raise ex.excError

def promote_dev_rw(dev, log=None):
    for dev in dev_to_paths(dev, log=log):
       count = 0
       if dev_is_ro(dev):
           dev_rescan(dev, log=log)
           wait_for_dev_ready(dev, log=log)
           count += 1
       if count > 0:
           refresh_multipath(dev, log=log)

def loop_is_deleted(dev):
    if not which("losetup"):
        raise ex.excError("losetup must be installed")
    out, err, ret = justcall(["losetup", dev])
    if "(deleted)" in out:
        return True
    return False

def label_to_dev(label):
    """
       blkid can return a device slave of a drbd, as drbd is
       transparent wrt to signature detection. Detect this case
       and return the holding device. Otherwise return None.
    """
    if label in label_to_dev_cache:
        return label_to_dev_cache[label]

    if not which("blkid"):
        return
    out, err, ret = justcall(["blkid", "-t", label])
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

    from rcDevTreeLinux import DevTree
    tree = DevTree()
    tree.load()
    devs = set([ tree.get_dev_by_devpath(devp) for devp in devps ]) - set([None])
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

def check_ping(addr, timeout=5, count=1):
    if ':' in addr:
        ping = 'ping6'
    else:
        ping = 'ping'
    cmd = [ping, '-c', repr(count),
                 '-W', repr(timeout),
                 '-w', repr(timeout),
                 addr]
    out, err, ret = justcall(cmd)
    if ret == 0:
        return True
    return False

def lv_exists(self, device):
    if qcall(['lvs', device]) == 0:
        return True
    return False

def lv_info(self, device):
    (ret, buff, err) = self.call(['lvs', '-o', 'vg_name,lv_name,lv_size', '--noheadings', '--units', 'm', device])
    if ret != 0:
        return (None, None, None)
    info = buff.split()
    if 'M' in info[2]:
        lv_size = float(info[2].split('M')[0])
    elif 'm' in info[2]:
        lv_size = float(info[2].split('m')[0])
    else:
        self.log.error("%s output does not have the expected unit (m or M)"%' '.join(cmd))
        ex.excError
    return (info[0], info[1], lv_size)

def get_partition_parent(dev):
    syspath = '/sys/block/*/' + os.path.basename(dev)
    l = glob.glob(syspath)
    if len(l) == 1:
        return '/dev/'+l[0].split('/')[3]
    return None

def devs_to_disks(self, devs=set([])):
    """ If PV is a device map, replace by its sysfs name (dm-*)
        If device map has slaves, replace by its slaves
    """
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
            self.log.debug("skip loop device %s from disklist"%dev)
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

