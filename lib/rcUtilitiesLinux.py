import os
import re
from rcUtilities import call, qcall

def major(driver):
    path = os.path.join(os.path.sep, 'proc', 'devices')
    try:
        f = open(path)
    except:
        raise
    for line in f.readlines():
        words = line.split()
        if len(words) == 2 and words[1] == driver:
            f.close()
            return int(words[0])
    f.close()
    raise

def get_blockdev_sd_slaves(syspath):
    slaves = set()
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
    (ret, out) = call(cmd)
    if ret == 0:
        return True
    return False

def lv_exists(self, device):
    if qcall(['lvs', device]) == 0:
        return True
    return False

def lv_info(self, device):
    (ret, buff) = self.call(['lvs', '-o', 'vg_name,lv_name,lv_size', '--noheadings', '--units', 'm', device])
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


