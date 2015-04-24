import os
import re
import glob
from rcUtilities import call, qcall, justcall

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
    out, err,ret = justcall(cmd)
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
            self.log.error("can not stat %s" % dev)
            raise
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

