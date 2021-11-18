import os
import threading
import time

from utilities.proc import justcall

from .node import Node as BaseNode


class Node(BaseNode):
    @staticmethod
    def get_tid():
        return "%s.%s" % (os.getpid(), threading.current_thread().ident)

    def sys_reboot(self, delay=0):
        if delay:
            self.log.info("sysrq reboot in %s seconds", delay)
            time.sleep(delay)
        justcall(['reboot', '-q'])

    def sys_crash(self, delay=0):
        if delay:
            self.log.info("sysrq crash in %s seconds", delay)
            time.sleep(delay)
        justcall(['halt', '-q'])

    def shutdown(self):
        cmd = ["init", "5"]
        self.vcall(cmd)

    def _reboot(self):
        cmd = ["init", "6"]
        self.vcall(cmd)

    def stats_meminfo(self):
        """
        return {
            "swap_total": int value in MB
            "swap_avail":
            "mem_total": int value in MB
            "mem_avail": %
        }
        """
        swap_total, swap_avail = self._get_swap()
        mem_total, mem_avail = self._get_mem()
        return {
            "swap_total": swap_total,
            "swap_avail": swap_avail,
            "mem_total": mem_total,
            "mem_avail": mem_avail
        }

    @staticmethod
    def _get_swap():
        """:returns swap_total, swap_avail or 0, 0 if errors

        where swap_avail is percentages, swap_total is MB
        Those value are computed from command 'swap -l' output

        swapfile             dev  swaplo blocs   libres
        /dev/dsk/c0d0s1     102,1       8 2425800 2425800
        """
        out, err, ret = justcall(["swap", "-l"])
        if ret != 0:
            return 0, 0
        swap_avail = 0
        swap_total = 0
        for line in out.splitlines():
            if line[0] != "/":
                continue
            elem = line.split()
            swap_avail += int(elem[4]) // 2
            swap_total += int(elem[3]) // 2
        if swap_total > 0:
            return swap_total // 1024, (100 * swap_avail) // swap_total
        else:
            return 0, 0

    @staticmethod
    def _get_mem():
        """
        :returns mem_total, mem_avail or 0, 0 if errors

        where mem_avail is percentages, mem_total is MB
        Those value are computed from command: 'kstat -n system_pages -p' output,

        unix:0:system_pages:availrmem   186832
        unix:0:system_pages:class       pages
        unix:0:system_pages:crtime      35,300879878
        unix:0:system_pages:desfree     8191
        unix:0:system_pages:desscan     25
        unix:0:system_pages:econtig     18446744073646514176
        unix:0:system_pages:fastscan    524231
        unix:0:system_pages:freemem     51757
        unix:0:system_pages:kernelbase  18446604435732824064
        unix:0:system_pages:lotsfree    16382
        unix:0:system_pages:minfree     4095
        unix:0:system_pages:nalloc      2007716475
        unix:0:system_pages:nalloc_calls        77545
        unix:0:system_pages:nfree       2003516163
        unix:0:system_pages:nfree_calls 67462
        unix:0:system_pages:nscan       0
        unix:0:system_pages:pagesfree   51757
        unix:0:system_pages:pageslocked 823743
        unix:0:system_pages:pagestotal  1048463
        unix:0:system_pages:physmem     1048463
        unix:0:system_pages:pp_kernel   861352
        unix:0:system_pages:slowscan    100
        unix:0:system_pages:snaptime    15173,890295243
        """
        import mmap

        out, err, ret = justcall(["kstat", "-n", "system_pages", "-p"])
        if ret != 0:
            return 0, 0
        raw_data = {}

        for line in out.splitlines():
            elem = line.split()
            raw_data[elem[0]] = elem[1]

        try:
            phys_mem = int(raw_data.get("unix:0:system_pages:physmem", "0"))
            avail_r_mem = int(raw_data.get("unix:0:system_pages:availrmem", "0"))
        except Exception:
            phys_mem = 0
            avail_r_mem = 0

        if phys_mem <= 0:
            return 0, 0
        mem_avail = 100 * avail_r_mem // phys_mem
        mem_total = phys_mem * mmap.PAGESIZE // 1024 // 1024

        return mem_total, mem_avail
