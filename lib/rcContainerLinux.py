import os
import re
import rcExceptions as ex
from rcUtilities import justcall, convert_size

default_cgroup_mntpt = '/cgroup'

def get_cgroup_mntpt(t):
    (out,err,ret) = justcall(['mount'])
    if ret != 0:
        return None
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 6:
            continue
        if l[4] == 'cgroup':
            mntopts = re.split('\W+',l[5])
            for opt in mntopts:
                if t == opt:
                    return l[2]
    return None

def cgroup_capable(res):
    kconf = os.path.join(os.sep, 'lib', 'modules',
                         os.uname()[2], 'build', '.config')
    if not os.path.exists(kconf):
        kconf = os.path.join(os.sep, 'boot', 'config-'+os.uname()[2])
    if not os.path.exists(kconf):
        res.log.info("can not detect if system supports containerization")
        return False
    with open(kconf, 'r') as f:
        for line in f.readlines():
            l = line.split('=')
            if len(l) != 2:
                continue
            if l[0] == 'CONFIG_CGROUPS' and l[1] == 'y\n':
                return True
    res.log.info("system does not support containerization")
    return False

def set_task(o, t):
    o.log.debug("set_task : start %s" %(t))
    cgp = get_cgroup_path(o, t)
    path = os.path.join(cgp, "tasks")
    pid = str(os.getpid())
    with open(path, 'r') as f:
        buff = f.read()
    if pid in buff.split():
        return
    try:
        o.log.debug("set_task : open path %s for writing" %(path))
        with open(path, 'w') as f:
            f.write(pid)
    except Exception as e:
        if hasattr(e, "errno") and e.errno == 28:
            # No space left on device
            # means the cgroup has not been initialized with caps yet
            pass
        else:
            raise

def set_cgroup(o, t, name, key, force=False):
    o.log.debug("set_cgroup : start %s, %s, %s, %s" %(t, name, key, force))
    if not hasattr(o, "containerize_settings"):
        return
    if key not in o.containerize_settings:
        return
    value = o.containerize_settings[key]
    cgp = get_cgroup_path(o, t)
    if value is None:
        return
    if not force and get_cgroup(o, t, name).strip() == str(value):
        return
    path = os.path.join(cgp, name)
    if not os.path.exists(path):
        raise ex.excError("can not find %s"%path)
    if hasattr(o, "log"):
        log = o.log
    elif hasattr(o, "svc"):
        log = o.svc.log
    try:
        with open(path, 'w') as f:
            f.write(str(value))
        log.info('/bin/echo %s > %s'%(value, path))
    except Exception as e:
        log.warning("failed to set container setting %s to %s" % (value, path))

def get_cgroup(o, t, name):
    o.log.debug("get_cgroup : start %s, %s" %(t, name))
    cgp = get_cgroup_path(o, t)
    path = os.path.join(cgp, name)
    if not os.path.exists(path):
        raise ex.excError("can not find %s"%path)
    with open(path, 'r') as f:
        buff = f.read()
    return buff

def set_cpu_quota(o):
    if not hasattr(o, "containerize_settings"):
        return
    o.log.debug("set_cpu_quota : start <%s>"%(o.containerize_settings))

    if 'cpu_quota' not in o.containerize_settings:
        return

    period = int(get_cgroup(o, 'cpu', 'cpu.cfs_period_us'))
    v = o.containerize_settings["cpu_quota"]

    if "@" in v:
        try:
            quota, cores = v.split("@")
        except Exception as e:
            raise ex.excError("malformed cpu quota: %s (%s)" % (v, str(e)))
    else:
        cores = 1
        quota = v

    if cores == "all":
        import rcAssetLinux
        cores = int(rcAssetLinux.Asset(None)._get_cpu_cores())
    else:
        cores = int(cores)

    total_us = period * cores

    if "%" in quota:
        quota = int(quota.strip("%"))
        tgt_val = total_us * quota // 100
    else:
        tgt_val = int(quota)
    cur_val = int(get_cgroup(o, 'cpu', 'cpu.cfs_quota_us'))

    if tgt_val == cur_val:
        return

    o.containerize_settings["cpu_cfs_quota_us"] = tgt_val
    set_cgroup(o, 'cpu', 'cpu.cfs_quota_us', 'cpu_cfs_quota_us')

def set_mem_cgroup(o):
    if not hasattr(o, "containerize_settings"):
        return
    o.log.debug("set_mem_cgroup : start <%s>"%(o.containerize_settings))

    if 'mem_limit' in o.containerize_settings:
        mem_limit = convert_size(o.containerize_settings['mem_limit'], _to="", _round=4096)
        o.containerize_settings['mem_limit'] = mem_limit
    else:
        mem_limit = None

    if 'vmem_limit' in o.containerize_settings:
        vmem_limit = convert_size(o.containerize_settings['vmem_limit'], _to="", _round=4096)
        o.containerize_settings['vmem_limit'] = vmem_limit
    else:
        vmem_limit = None

    if hasattr(o, "log"):
        log = o.log
    elif hasattr(o, "svc"):
        log = o.svc.log

    #
    # validate memory limits sanity and order adequately the resize
    # depending on increase/decrease of limits
    #
    try:
        cur_vmem_limit = int(get_cgroup(o, 'memory', 'memory.memsw.limit_in_bytes'))
    except ex.excError:
        cur_vmem_limit = None
    if mem_limit is not None and vmem_limit is not None:
        if mem_limit > vmem_limit:
            log.error("container_vmem_limit must be greater than container_mem_limit")
            raise ex.excError
        if mem_limit > cur_vmem_limit:
            set_cgroup(o, 'memory', 'memory.memsw.limit_in_bytes', 'vmem_limit')
            set_cgroup(o, 'memory', 'memory.limit_in_bytes', 'mem_limit')
        else:
            set_cgroup(o, 'memory', 'memory.limit_in_bytes', 'mem_limit')
            set_cgroup(o, 'memory', 'memory.memsw.limit_in_bytes', 'vmem_limit')
    elif mem_limit is not None:
        if cur_vmem_limit and mem_limit > cur_vmem_limit:
            log.error("container_mem_limit must not be greater than current container_vmem_limit (%d)"%cur_vmem_limit)
            raise ex.excError
        set_cgroup(o, 'memory', 'memory.limit_in_bytes', 'mem_limit')
    elif vmem_limit is not None:
        cur_mem_limit = int(get_cgroup(o, 'memory', 'memory.limit_in_bytes'))
        if vmem_limit < cur_mem_limit:
            log.error("container_vmem_limit must not be lesser than current container_mem_limit (%d)"%cur_mem_limit)
            raise ex.excError
        set_cgroup(o, 'memory', 'memory.memsw.limit_in_bytes', 'vmem_limit')

def get_cgroup_path(o, t, create=True):
    o.log.debug("get_cgroup_path : start %s, %s"%(t, create))
    cgroup_mntpt = get_cgroup_mntpt(t)
    if hasattr(o, "svcname"):
        svcname = o.svcname
    else:
        svcname = o.svc.svcname
    if cgroup_mntpt is None:
        raise ex.excError("cgroup fs with option %s is not mounted" % t)
    elements = [cgroup_mntpt, svcname]
    if hasattr(o, "rset") and o.rset is not None:
        elements.append(o.rset.rid)
    if hasattr(o, "rid") and o.rid is not None:
        elements.append(o.rid.replace("#", "."))
    cgp = os.path.join(*elements)
    if hasattr(o, "log"):
        log = o.log
    elif hasattr(o, "svc"):
        log = o.svc.log
    if not os.path.exists(cgp) and create:
        log.info("create cgroup %s" % cgp)
        os.makedirs(cgp)
    return cgp

def freeze(o):
    return freezer(o, "FROZEN")

def thaw(o):
    return freezer(o, "THAWED")

def freezer_lxc(o, a):
    # lxc-init children tasks are in the freezer cgroup lxc/<container>/
    cgroup_mntpt = get_cgroup_mntpt("freezer")
    cgp = os.path.join(cgroup_mntpt, "lxc", o.name)
    if os.path.exists(cgp):
        _freezer(o, a, cgp)

def freezer(o, a):
    if not cgroup_capable(o):
        return
    cgp = get_cgroup_path(o, "freezer")
    _freezer(o, a, cgp)
    if o.type == "container.lxc":
        freezer_lxc(o, a)
    if hasattr(o, "svcname"):
        for r in o.get_resources("container.lxc"):
            freezer_lxc(r, a)

def _freezer(o, a, cgp):
    path = os.path.join(cgp, "freezer.state")
    if hasattr(o, "log"):
        log = o.log
    elif hasattr(o, "svc"):
        log = o.svc.log
    if not os.path.exists(path):
        raise ex.excError("freezer control file not found: %s"%path)
    try:
        with open(path, "r") as f:
            buff = f.read()
    except Exception as e:
        raise ex.excError(str(e))
    buff = buff.strip()
    if buff == a:
        log.info("%s is already %s" % (path, a))
        return
    elif buff == "FREEZING":
        log.info("%s is currently FREEZING" % path)
        return
    try:
        with open(path, "w") as f:
            buff = f.write(a)
    except Exception as e:
        raise ex.excError(str(e))
    log.info("%s on %s submited succesfully" % (a, path))

def get_freeze_state(o):
    if not cgroup_capable(o):
        return False
    cgp = get_cgroup_path(o, "freezer", create=False)
    path = os.path.join(cgp, "freezer.state")
    if not os.path.exists(path):
        return
    with open(path, "r") as f:
        buff = f.read()
    buff = buff.strip()
    return buff

def frozen(res):
    for o in (res.svc, res.rset, res):
        try:
            state = get_freeze_state(o)
        except Exception as e:
            state = None
        if state in ("FROZEN", "FREEZING"):
            try:
                name = o.svcname
            except:
                name = o.rid
            res.status_log("container %s is %s" % (name, state))
            return True
    return False

def containerize(res):
    if not cgroup_capable(res):
        return

    _containerize(res.svc)
    _containerize(res.rset)
    _containerize(res)

def _containerize(o):
    if o is None:
        return
    try:
        set_task(o, 'systemd')
        set_task(o, 'cpu')
        set_task(o, 'cpuset')
        set_task(o, 'memory')
        set_task(o, 'blkio')
        set_task(o, 'freezer')
        set_cgroup(o, 'cpuset', 'cpuset.cpus', 'cpus')
        set_cgroup(o, 'cpu', 'cpu.shares', 'cpu_shares')
        set_cgroup(o, 'cpuset', 'cpuset.mems', 'mems')
        set_mem_cgroup(o)
        set_cpu_quota(o)
    except Exception as e:
        try:
            name = o.svcname
        except:
            name = o.rid
        raise ex.excError("containerization in '%s' cgroup failed: %s"%(name, str(e)))
