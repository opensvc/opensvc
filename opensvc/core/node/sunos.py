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
        ret, out, err = self.vcall(cmd)

    def _reboot(self):
        cmd = ["init", "6"]
        ret, out, err = self.vcall(cmd)

    def stats_meminfo(self):
        """
        Memory sizes are store in MB.
        Avails are percentages.

        # kstat -n system_pages -p 
        unix:0:system_pages:availrmem   48190
        unix:0:system_pages:physmem     257948

        # swap -l 
        swapfile             dev  swaplo blocs   libres
        /dev/dsk/c0d0s1     102,1       8 2425800 2425800
        """
        import mmap

        raw_data = {}
        data = {}
        out, err, ret = justcall(["swap", "-l"])
        swap_avail = 0
        swap_total = 0
        for line in out.splitlines():
            if line[0] != "/":
                continue
            elem = line.split()
            swap_avail += int(elem[4]) // 2
            swap_total += int(elem[3]) // 2
        data["swap_avail"] = 100 - swap_avail // swap_total
        data["swap_total"] = swap_total // 1024

        out, err, ret = justcall(["kstat", "-n", "system_pages", "-p"])
        for line in out.splitlines():
            elem = line.split()
            raw_data[elem[0]] = elem[1]

        data["mem_total"] = int(raw_data["unix:0:system_pages:physmem"]) * mmap.PAGESIZE // 1024 // 1024
        data["mem_avail"] = 100 * int(raw_data["unix:0:system_pages:availrmem"]) // int(raw_data["unix:0:system_pages:physmem"])
        return data
