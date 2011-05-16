import os
import rcExceptions as ex
from rcUtilities import justcall

default_cgroup_mntpt = '/cgroup'

def get_cgroup_mntpt():
    (out,err,ret) = justcall(['mount'])
    if ret != 0:
        return None
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 6:
            continue
        if l[4] == 'cgroup':
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

def set_cgroup(res, name, value):
    if value is None:
        return
    path = os.path.join(res.cgroup, name)
    if not os.path.exists(path):
        res.log.error("can not find %s"%path)
        raise ex.excError
    res.log.info('/bin/echo %s > %s'%(value, path))
    with open(path, 'w') as f:
        f.write(str(value))

def get_cgroup(res, name):
    path = os.path.join(res.cgroup, name)
    if not os.path.exists(path):
        res.log.error("can not find %s"%path)
        raise ex.excError
    with open(path, 'r') as f:
        buff = f.read()
    return buff

def set_mem_cgroup(res):
    if hasattr(res.svc, 'container_mem_limit'):
        mem_limit = int(res.svc.container_mem_limit)
    else:
        mem_limit = None

    if hasattr(res.svc, 'container_vmem_limit'):
        vmem_limit = int(res.svc.container_vmem_limit)
    else:
        vmem_limit = None

    #
    # validate memory limits sanity and order adequately the resize
    # depending on increase/decrease of limits
    #
    if mem_limit is not None and vmem_limit is not None:
        if mem_limit > vmem_limit:
            res.log.error("container_vmem_limit must be greater than container_mem_limit")
            raise ex.excError
        cur_vmem_limit = int(get_cgroup(res, 'memory.memsw.limit_in_bytes'))
        if mem_limit > cur_vmem_limit:
            set_cgroup(res, 'memory.memsw.limit_in_bytes', vmem_limit)
            set_cgroup(res, 'memory.limit_in_bytes', mem_limit)
        else:
            set_cgroup(res, 'memory.limit_in_bytes', mem_limit)
            set_cgroup(res, 'memory.memsw.limit_in_bytes', vmem_limit)
    elif mem_limit is not None:
        cur_vmem_limit = int(get_cgroup(res, 'memory.memsw.limit_in_bytes'))
        if mem_limit > cur_vmem_limit:
            res.log.error("container_mem_limit must not be greater than current container_vmem_limit (%d)"%cur_vmem_limit)
            raise ex.excError
        set_cgroup(res, 'memory.limit_in_bytes', mem_limit)
    elif vmem_limit is not None:
        cur_mem_limit = int(get_cgroup(res, 'memory.limit_in_bytes'))
        if vmem_limit < cur_mem_limit:
            res.log.error("container_vmem_limit must not be lesser than current container_mem_limit (%d)"%cur_mem_limit)
            raise ex.excError
        set_cgroup(res, 'memory.memsw.limit_in_bytes', vmem_limit)

def containerize(res):
    if res.svc.svcmode == 'lxc':
        return
    if not cgroup_capable(res):
        return

    cgroup_mntpt = get_cgroup_mntpt()
    if cgroup_mntpt is None:
        cgroup_mntpt = default_cgroup_mntpt
        if not os.path.exists(cgroup_mntpt):
            res.log.info('mkdir %s'%cgroup_mntpt)
            os.makedirs(cgroup_mntpt)
        (ret, out, err) = res.vcall(['mount', '-t', 'cgroup', 'none', cgroup_mntpt])
        if ret != 0:
            raise ex.excError

    res.cgroup = os.path.join(cgroup_mntpt, res.svc.svcname)
    if not os.path.exists(res.cgroup):
        os.makedirs(res.cgroup)

    if hasattr(res.svc, 'container_cpus'):
        v_cpus = str(res.svc.container_cpus)
    else:
        # default to all cpus
        import multiprocessing
        v_cpus = '0-%d'%(multiprocessing.cpu_count()-1)

    if hasattr(res.svc, 'container_mems'):
        v_mems = str(res.svc.container_mems)
    else:
        # default to all mems
        v_mems = '0'

    if hasattr(res.svc, 'container_cpu_share'):
        cpu_share = res.svc.container_cpu_share
    else:
        cpu_share = None

    pid = os.getpid()

    try:
        set_cgroup(res, 'cpuset.cpus', v_cpus)
        set_cgroup(res, 'cpu.shares', cpu_share)
        set_cgroup(res, 'cpuset.mems', v_mems)
        set_mem_cgroup(res)
        set_cgroup(res, 'tasks', pid)
    except:
        res.log.error("containerization in '%s' cgroup failed"%res.svc.svcname)
        raise ex.excError
    res.log.info("containerized in '%s' cgroup, with limits cpu[%s], mem[%s]:"%(res.svc.svcname, v_cpus, v_mems))
