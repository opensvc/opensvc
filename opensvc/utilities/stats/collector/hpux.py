import datetime
import os
from subprocess import *

from env import Env
from utilities.proc import justcall, is_exe


def collect(node):
    now = str(datetime.datetime.now())

    def fs_u():
        vars = ['date',
                'nodename',
                'mntpt',
                'size',
                'used']

        cmd = ['df', '-lP']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return
        lines = out.split('\n')
        if len(lines) < 2:
            return
        vals = []
        for line in lines[1:]:
            l = line.split()
            if len(l) != 6:
                continue
            vals.append([now, node.nodename, l[5], l[1], l[4].replace('%', '')])
        return vars, vals

    def glance_running(cmd_str):
        (out, err, ret) = justcall(['ps', '-ef'])
        if ret != 0:
            print('ps error')
            return

        for line in out.split('\n'):
            l = line.split()
            if len(l) < 6:
                continue
            if cmd_str in ' '.join(l[6:]):
                return True

        return False

    def run_glance():
        glance = '/opt/perf/bin/glance'
        syn = os.path.join(Env.paths.pathtmp, 'glance.syntax')
        now = datetime.datetime.now()
        iterations = (23 - now.hour) * 6 + (60 - now.minute) // 10
        cmd = ['/opt/perf/bin/glance', '-aos', syn, '-j', '600', '-iterations']
        cmd_str = ' '.join(cmd)

        if not is_exe(glance):
            print('glance executable not found')
            return

        if glance_running(cmd_str):
            print('glance is already running')
            return

        buff = """print GBL_STATTIME," ",
    // usr
    0.00+GBL_CPU_NORMAL_UTIL+GBL_CPU_REALTIME_UTIL," ",
    // nice
    0.00+GBL_CPU_NICE_UTIL+GBL_CPU_NNICE_UTIL," ",
    // sys
    0.00+GBL_CPU_SYSCALL_UTIL+GBL_CPU_CSWITCH_UTIL+GBL_CPU_TRAP_UTIL+GBL_CPU_VFAULT_UTIL," ",
    // irq
    0.00+GBL_CPU_INTERRUPT_UTIL," ",
    // wait
    0.00+GBL_CPU_WAIT_UTIL," ",
    // idle
    0.00+GBL_CPU_IDLE_UTIL-GBL_CPU_WAIT_UTIL," ",
    
    // mem
    0+GBL_MEM_PHYS," ",
    0+GBL_MEM_FREE," ",
    0+GBL_MEM_CACHE," ",
    0+GBL_MEM_FILE_PAGE_CACHE," ",
    0+GBL_MEM_SYS," ",
    0+GBL_MEM_USER," ",
    
    // swap
    0+GBL_MEM_SWAP," ",
    0+GBL_SWAP_SPACE_AVAIL-GBL_MEM_PHYS," ",
    
    // load
    GBL_LOADAVG," ",
    GBL_LOADAVG5," ",
    GBL_LOADAVG15," ",
    GBL_CPU_QUEUE," ",
    
    // process list
    TBL_PROC_TABLE_USED," ",
    
    // disk io
    GBL_DISK_PHYS_READ_RATE," ",
    GBL_DISK_PHYS_WRITE_RATE," ",
    
    // disk kB/s
    GBL_DISK_PHYS_READ_BYTE_RATE," ",
    GBL_DISK_PHYS_WRITE_BYTE_RATE
    """
        try:
            with open(syn, 'w') as f:
                f.write(buff)
        except:
            print('error writing %s' % syn)
            return

        collect_d = os.path.join(Env.paths.pathvar, "stats")
        collect_f = 'glance%0.2d' % now.day
        collect_p = os.path.join(collect_d, collect_f)

        if os.path.exists(collect_p):
            mtime = os.stat(collect_p).st_mtime
            if datetime.datetime.fromtimestamp(mtime) < now - datetime.timedelta(days=1):
                os.unlink(collect_p)

        _cmd = 'nohup %s %d >>%s &' % (cmd_str, iterations, collect_p)
        Popen(_cmd, shell=True, stdout=PIPE, stderr=PIPE)

    run_glance()
    fs_u_data = fs_u()
    if fs_u_data is not None:
        node.collector.call('push_stats_fs_u', fs_u_data)
