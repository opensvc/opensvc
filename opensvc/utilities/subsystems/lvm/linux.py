from env import Env
from utilities.cache import cache
from utilities.proc import justcall

@cache("lvs.attr")
def get_lvs_attr():
    cmd = [Env.syspaths.lvs, '-o', 'vg_name,lv_name,lv_attr', '--noheadings', '--separator=;']
    out, err, ret = justcall(cmd)
    data = {}
    for line in out.splitlines():
        l = line.split(";")
        if len(l) != 3:
            continue
        vgname = l[0].strip()
        lvname = l[1].strip()
        attr = l[2].strip()
        if vgname not in data:
            data[vgname] = {}
        data[vgname][lvname] = attr
    return data

