import os
import re
import glob
import core.exceptions as ex
from utilities.converters import convert_size

UNIFIED_MNT = "/sys/fs/cgroup/unified"
UNIFIED = os.path.exists(UNIFIED_MNT)
DRIVER_BASENAME = 'pg'

CONTROLLERS = [
    "blkio",
    "cpu",
    "cpuacct",
    "cpuset",
    "devices",
    "freezer",
    "hugetlb",
    "memory",
    "net_cls",
    "perf_event",
    "pids",
    "rdma",
    "systemd",
]

def get_cgroup_mntpt(t):
    if UNIFIED and t is None:
        return UNIFIED_MNT
    p = '/proc/mounts'
    if not os.path.exists(p):
        return None
    with open(p, 'r') as f:
        buff = f.read()
    for line in buff.split('\n'):
        if 'cgroup' not in line:
            continue
        l = line.split()
        if len(l) < 6:
            continue
        if l[2] == 'cgroup':
            mntopts = re.split(r'\W+',l[3])
            for opt in mntopts:
                if t == opt:
                    return l[1]
    return None

def cgroup_capable(res):
    if os.path.exists("/proc/1/cgroup"):
        return True
    kconf = os.path.join(os.sep, 'lib', 'modules',
                         os.uname()[2], 'build', '.config')
    if not os.path.exists(kconf):
        kconf = os.path.join(os.sep, 'boot', 'config-'+os.uname()[2])
    if not os.path.exists(kconf):
        res.log.info("can not detect if system supports process groups")
        return False
    with open(kconf, 'r') as f:
        for line in f.readlines():
            l = line.split('=')
            if len(l) != 2:
                continue
            if l[0] == 'CONFIG_CGROUPS' and l[1] == 'y\n':
                return True
    res.log.info("system does not support process groups")
    return False

def get_task_file(cgp):
    if cgp.startswith(UNIFIED_MNT):
        return os.path.join(cgp, "cgroup.procs")
    else:
        return os.path.join(cgp, "tasks")

def set_task(o, t):
    o.log.debug("set_task : start %s" %(t))
    cgp = get_cgroup_path(o, t)
    path = get_task_file(cgp)
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
        if hasattr(e, "errno") and getattr(e, "errno") == 28:
            # No space left on device
            # means the cgroup has not been initialized with caps yet
            pass
        else:
            raise

def set_cgroup(o, *args, **kwargs):
    try:
        _set_cgroup(o, *args, **kwargs)
    except Exception as exc:
        o.log.warning(exc)

def _set_cgroup(o, t, name, key, force=False):
    o.log.debug("set_cgroup : start %s, %s, %s, %s" %(t, name, key, force))
    if not hasattr(o, "pg_settings"):
        return
    if key not in o.pg_settings:
        return
    value = o.pg_settings[key]
    cgp = get_cgroup_path(o, t)
    if value is None:
        return
    if name == "memory.oom_control":
        current = get_cgroup(o, t, name).split(os.linesep)[0].split()[-1]
    else:
        current = get_cgroup(o, t, name).strip()
    if not force and current == str(value):
        return
    path = os.path.join(cgp, name)
    if not os.path.exists(path):
        raise ex.Error("can not find %s"%path)
    if hasattr(o, "log"):
        log = o.log
    elif hasattr(o, "svc"):
        log = o.svc.log
    try:
        with open(path, 'w') as f:
            f.write(str(value))
        log.info('/bin/echo %s > %s'%(value, path))
    except Exception as e:
        log.warning("failed to set process group setting %s to %s" % (value, path))

def get_cgroup(o, t, name):
    o.log.debug("get_cgroup : start %s, %s" %(t, name))
    cgp = get_cgroup_path(o, t)
    path = os.path.join(cgp, name)
    if not os.path.exists(path):
        raise ex.Error("can not find %s"%path)
    with open(path, 'r') as f:
        buff = f.read()
    return buff

def set_cpu_quota(o):
    try:
        v = str(o.pg_settings["cpu_quota"])
    except (AttributeError, KeyError):
        return
    o.log.debug("set_cpu_quota : start <%s>", v)

    period = int(get_cgroup(o, 'cpu', 'cpu.cfs_period_us'))

    if "@" in v:
        try:
            quota, threads = v.split("@")
        except Exception as e:
            raise ex.Error("malformed cpu quota: %s (%s)" % (v, str(e)))
    else:
        threads = 1
        quota = v

    if threads == "all":
        from utilities.asset import Asset
        threads = int(Asset(None)._get_cpu_threads())
    else:
        threads = int(threads)

    total_us = period * threads

    if "%" in quota:
        quota = int(quota.strip("%"))
        tgt_val = total_us * quota // 100
    else:
        tgt_val = int(quota)
    cur_val = int(get_cgroup(o, 'cpu', 'cpu.cfs_quota_us'))

    if tgt_val == cur_val:
        return

    o.pg_settings["cpu_cfs_quota_us"] = tgt_val
    set_cgroup(o, 'cpu', 'cpu.cfs_quota_us', 'cpu_cfs_quota_us')

def set_mem_cgroup(o):
    if not hasattr(o, "pg_settings"):
        return
    o.log.debug("set_mem_cgroup : start <%s>"%(o.pg_settings))

    if 'mem_limit' in o.pg_settings:
        mem_limit = convert_size(o.pg_settings['mem_limit'], _to="", _round=4096)
        o.pg_settings['mem_limit'] = mem_limit
    else:
        mem_limit = None

    if 'vmem_limit' in o.pg_settings:
        vmem_limit = convert_size(o.pg_settings['vmem_limit'], _to="", _round=4096)
        o.pg_settings['vmem_limit'] = vmem_limit
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
    except ex.Error:
        cur_vmem_limit = None
    if mem_limit is not None and vmem_limit is not None:
        if mem_limit > vmem_limit:
            log.error("pg_vmem_limit must be greater than pg_mem_limit")
            raise ex.Error
        if mem_limit > cur_vmem_limit:
            set_cgroup(o, 'memory', 'memory.memsw.limit_in_bytes', 'vmem_limit')
            set_cgroup(o, 'memory', 'memory.limit_in_bytes', 'mem_limit')
        else:
            set_cgroup(o, 'memory', 'memory.limit_in_bytes', 'mem_limit')
            set_cgroup(o, 'memory', 'memory.memsw.limit_in_bytes', 'vmem_limit')
    elif mem_limit is not None:
        if cur_vmem_limit and mem_limit > cur_vmem_limit:
            log.error("pg_mem_limit must not be greater than current pg_vmem_limit (%d)"%cur_vmem_limit)
            raise ex.Error
        set_cgroup(o, 'memory', 'memory.limit_in_bytes', 'mem_limit')
    elif vmem_limit is not None:
        cur_mem_limit = int(get_cgroup(o, 'memory', 'memory.limit_in_bytes'))
        if vmem_limit < cur_mem_limit:
            log.error("pg_vmem_limit must not be lesser than current pg_mem_limit (%d)"%cur_mem_limit)
            raise ex.Error
        set_cgroup(o, 'memory', 'memory.memsw.limit_in_bytes', 'vmem_limit')

def get_namespace(o):
    if hasattr(o, "namespace"):
        buff = o.namespace
    else:
        buff = o.svc.namespace
    return buff

def get_name(o):
    if hasattr(o, "path"):
        buff = o.name
    else:
        buff = o.svc.name
    return buff

def get_log(o):
    if hasattr(o, "log"):
        log = o.log
    elif hasattr(o, "svc"):
        log = o.svc.log
    else:
        log = None
    return log

def get_cgroup_ns_relpath(o, suffix=".slice"):
    namespace = get_namespace(o)
    elements = ["opensvc" + suffix]
    if namespace:
        elements.append(namespace + suffix)
    return os.path.join(*elements)

def get_cgroup_svc_relpath(o, suffix=".slice"):
    name = get_name(o)
    elements = [get_cgroup_ns_relpath(o, suffix=suffix)]
    elements.append(name + suffix)
    return os.path.join(*elements)

def get_cgroup_relpath(o, suffix=".slice"):
    if hasattr(o, "type") and o.type == "container.lxc" and \
       hasattr(o, "name") and not o.capable("cgroup_dir"):
        return os.path.join("lxc", o.name)

    if hasattr(o, "kind") and o.kind == "nscfg":
        elements = [get_cgroup_ns_relpath(o, suffix=suffix)]
    else:
        elements = [get_cgroup_svc_relpath(o, suffix=suffix)]
        if hasattr(o, "rset") and o.rset is not None:
            elements.append(o.rset.rid.replace(":", ".") + suffix)
        if hasattr(o, "rid") and o.rid is not None:
            elements.append(o.rid.replace("#", ".") + suffix)
    return os.path.join(*elements)

def get_cgroup_path(o, t, create=True):
    o.log.debug("get_cgroup_path : t=%s, create=%s"%(t, create))
    cgroup_mntpt = get_cgroup_mntpt(t)
    if cgroup_mntpt is None:
        raise ex.Error("cgroup fs with option %s is not mounted" % t)
    relpath = get_cgroup_relpath(o)
    cgp = os.sep.join([cgroup_mntpt, relpath])
    log = get_log(o)

    if not os.path.exists(cgp) and create:
        if hasattr(o, "cleanup_cgroup"):
            o.cleanup_cgroup(t)
        create_cgroup(cgp, log=log)
    return cgp

def remove_pg(o):
    log = o.log
    for t in CONTROLLERS:
        cgp = os.path.join(os.sep, "sys", "fs", "cgroup", t, get_cgroup_relpath(o))
        remove_cgroup(cgp, log)
        cgp = os.path.join(os.sep, "sys", "fs", "cgroup", t, get_cgroup_relpath(o, suffix=""))
        remove_cgroup(cgp, log)

def remove_cgroup(cgp, log):
    if not os.path.exists(cgp):
        return
    todo = [cgp]
    for dirpath, subdirs, _ in os.walk(cgp):
        for subdir in subdirs:
            path = os.path.join(dirpath, subdir)
            todo.append(path)
    for path in sorted(todo, reverse=True):
        try:
            os.rmdir(path)
            #log.info("removed %s", path)
        except Exception as exc:
            #print("lingering %s (%s)" % (path, exc))
            pass

def create_cgroup(cgp, log=None):
    try:
        os.makedirs(cgp)
    except OSError as exc:
        if exc.errno == 17:
            pass
        else:
            raise
    set_sysfs(cgp+"/cgroup.clone_children", "1")
    for parm in ("cpus", "mems"):
        parent_val = get_sysfs(cgp+"/../cpuset."+parm)
        set_sysfs(cgp+"/cpuset."+parm, parent_val, log=log)

def get_sysfs(path):
    try:
        with open(path, "r") as f:
            return f.read().rstrip("\n")
    except Exception:
        return

def set_sysfs(path, val, log=None):
    current_val = get_sysfs(path)
    if current_val is None:
        return
    if current_val == val:
        return
    
    if log:
        log.info("/bin/echo %s >%s" % (val, path))

    with open(path, "w") as f:
        return f.write(val)

def freeze(o):
    return freezer(o, "FROZEN")

def thaw(o):
    return freezer(o, "THAWED")

def pids(o, controller="memory"):
    cgp = get_cgroup_path(o, controller)
    _pids = set()
    fname = "cgroup.procs"
    for path, _, files in os.walk(cgp):
        fpath = os.path.join(path, fname)
        if fname not in files:
            continue
        try:
            with open(fpath, "r") as f:
                for pid in f.readlines():
                    _pids.add(pid.strip())
        except Exception:
            pass
    return list(_pids)

def kill(o):
    _pids = pids(o, controller="freezer")
    if hasattr(o, "log"):
        _o = o
    else:
        _o = o.svc

    if len(_pids) == 0:
        _o.log.info("no task to kill")
        return
    cmd = ["kill"] + list(_pids)
    _o.vcall(cmd)

    if hasattr(o, "path"):
        # lxc containers are not parented to the service cgroup
        # so we have to kill them individually
        for r in o.get_resources("container.lxc"):
            kill(r)


def freezer(o, a):
    if not cgroup_capable(o):
        return
    cgp = get_cgroup_path(o, "freezer")
    _freezer(o, a, cgp)
    if hasattr(o, "path"):
        # lxc containers are not parented to the service cgroup
        # so we have to freeze them individually
        for r in o.get_resources("container.lxc"):
            freezer(r, a)

def _freezer(o, a, cgp):
    path = os.path.join(cgp, "freezer.state")
    if hasattr(o, "log"):
        log = o.log
    elif hasattr(o, "svc"):
        log = o.svc.log
    if not os.path.exists(path):
        raise ex.Error("freezer control file not found: %s"%path)
    try:
        with open(path, "r") as f:
            buff = f.read()
    except Exception as e:
        raise ex.Error(str(e))
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
        raise ex.Error(str(e))
    log.info("%s on %s submitted successfully" % (a, path))

    # el6 kernel does not freeze child cgroups, as later kernels do
    for _cgp in glob.glob(cgp+"/*/*/freezer.state"):
        _cgp = os.path.dirname(_cgp)
        _freezer(o, a, _cgp)

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
                name = o.name
            except:
                name = o.rid
            res.status_log("process group %s is %s" % (name, state))
            return True
    return False

def create_pg(res):
    if not cgroup_capable(res):
        return

    _create_pg(res.svc)
    _create_pg(res.rset)
    _create_pg(res)

def set_controllers_task(o):
    for controller in CONTROLLERS:
        try:
            set_task(o, controller)
        except ex.Error:
            pass

def _create_pg(o):
    if o is None:
        return
    try:
        if UNIFIED:
            set_task(o, None)
        else:
            try:
                set_task(o, 'systemd')
            except:
                pass
        set_controllers_task(o)
        set_cgroup(o, 'cpuset', 'cpuset.cpus', 'cpus')
        set_cgroup(o, 'cpu', 'cpu.shares', 'cpu_shares')
        set_cgroup(o, 'cpuset', 'cpuset.mems', 'mems')
        set_cgroup(o, 'blkio', 'blkio.weight', 'blkio_weight')
        set_cgroup(o, 'memory', 'memory.swappiness', 'mem_swappiness')
        set_cgroup(o, 'memory', 'memory.oom_control', 'mem_oom_control')
        set_mem_cgroup(o)
        set_cpu_quota(o)
    except Exception as e:
        # not configured in kernel
        pass

##############################################################################
#
# Cgroup Stats
#
##############################################################################

def get_stats_mem(o):
    data = {}
    _data = {}
    cgp = get_cgroup_path(o, "memory", create=False)
    buff = get_sysfs(cgp+"/memory.stat")
    for line in buff.splitlines():
        k, v = line.split()
        _data[k] = int(v)
    data["total"] = _data.get("total_cache", 0) + _data.get("total_rss", 0) + _data.get("total_rss_huge", 0) + _data.get("total_shmem", 0)
    return data

def get_stats_cpu(o):
    data = {}
    cgp = get_cgroup_path(o, "cpu", create=False)
    data["time"] = int(get_sysfs(cgp+"/cpuacct.usage")) / 1000000000
    return data

def get_stats_blk(o):
    data = {}
    cgp = get_cgroup_path(o, "blkio", create=False)
    if not os.path.exists(cgp):
        raise ex.Error

    rb = 0
    wb = 0
    rio = 0
    wio = 0
    leaves = 0

    def get(path, filename):
        buff = get_sysfs(path+"/" + filename)
        r = 0
        w = 0
        for line in buff.splitlines():
            l = line.split()
            try:
                if l[1] == "Read":
                    r = int(l[2])
                elif l[1] == "Write":
                    w = int(l[2])
            except IndexError:
                pass
        return r, w

    for path, subdirs, _ in os.walk(cgp):
        if subdirs:
            continue
        leaves += 1
        r, w = get(path, "blkio.throttle.io_serviced")
        rio += r
        wio += w
        r, w = get(path, "blkio.throttle.io_service_bytes")
        rb += r
        wb += w
    if not leaves:
        raise IndexError
    return {
        "r": rio,
        "w": wio,
        "rb": rb,
        "wb": wb,
    }

def get_stats_tasks(o):
    cgp = get_cgroup_path(o, "cpu", create=False)
    if not os.path.exists(cgp):
        raise ex.Error
    count = 0
    for path, subdirs, _ in os.walk(cgp):
        if subdirs:
            continue
        count += len(get_sysfs(path+"/tasks").splitlines())
    return count

def get_stats_net(o):
    _pids = pids(o)
    if not _pids:
        raise IndexError
    ns_done = []
    data = {
        "r": 0,
        "w": 0,
        "rb": 0,
        "wb": 0,
    }
    for pid in _pids:
        try:
            fpath = "/proc/%s/ns/net" % pid
            ns = os.readlink(fpath)
            if ns in ns_done:
                continue
            fpath = "/proc/%s/net/dev" % pid
            with open(fpath, "r") as f:
                lines = f.read().split("\n")[2:-1]
            for line in lines:
                l = line.split()
                data["r"] += int(l[2])
                data["w"] += int(l[10])
                data["rb"] += int(l[1])
                data["wb"] += int(l[9])
            ns_done.append(ns)
        except Exception as exc:
            #print(fpath, exc, line, pid)
            continue
    return data

def get_stats_created(o):
    data = {}
    cgp = get_cgroup_path(o, "cpu", create=False)
    return os.path.getmtime(cgp)

STATS = [
    ("cpu", get_stats_cpu),
    ("mem", get_stats_mem),
    ("blk", get_stats_blk),
    ("tasks", get_stats_tasks),
    ("net", get_stats_net),
    ("created", get_stats_created),
]

def get_stats(o):
    """
    Return all cgroup stats
    """
    data = {}
    for key, fn in STATS:
        try:
            data[key] = fn(o)
        except Exception:
            pass
    return data
