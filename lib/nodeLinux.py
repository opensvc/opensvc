import os
import time
from itertools import islice

from rcUtilitiesLinux import get_tid
from rcUtilities import lazy, justcall
import node

class Node(node.Node):
    def still_alive(self):
        try:
            with open("/proc/sys/kernel/sysrq", "r") as ofile:
                buff = ofile.read()
        except Exception:
            buff = "<unknown>"
        self.log.error("still alive ... maybe crashing is ignored by kernel.sysrq=%s (check dmesg)" % buff)
        time.sleep(1)
        self.freeze()

    def sys_reboot(self, delay=0):
        if delay:
            self.log.info("sysrq reboot in %s seconds", delay)
            time.sleep(delay)
        with open("/proc/sysrq-trigger", "w") as ofile:
            ofile.write("b")
        self.still_alive()
        time.sleep(1)
        self.freeze()

    def sys_crash(self, delay=0):
        if delay:
            self.log.info("sysrq crash in %s seconds", delay)
            time.sleep(delay)
        with open("/proc/sysrq-trigger", "w") as ofile:
            ofile.write("c")
        self.still_alive()

    def shutdown(self):
        cmd = ["shutdown", "-h", "now"]
        ret, out, err = self.vcall(cmd)

    def _reboot(self):
        cmd = ["reboot"]
        ret, out, err = self.vcall(cmd)

    def stats_meminfo(self):
        """
        Memory sizes are store in MB.
        Avails are percentages.
        """
        raw_data = {}
        data = {}
        with open("/proc/meminfo", "r") as ofile:
            for line in ofile.readlines():
                elem = line.split()
                if len(elem) < 2:
                    continue
                raw_data[elem[0].rstrip(":")] = int(elem[1])
        data["mem_total"] = raw_data["MemTotal"] // 1024
        try:
            data["mem_avail"] = 100 * raw_data["MemAvailable"] // raw_data["MemTotal"]
        except KeyError:
            data["mem_avail"] = 100 * (raw_data["MemFree"] + raw_data["Cached"] + raw_data.get("SReclaimable", 0)) // raw_data["MemTotal"]
        data["swap_total"] = raw_data["SwapTotal"] // 1024
        try:
            data["swap_avail"] = 100 * raw_data["SwapFree"] // raw_data["SwapTotal"]
        except:
            data["swap_avail"] = 0
        return data

    @lazy
    def user_hz(self):
        return os.sysconf(os.sysconf_names['SC_CLK_TCK'])

    @staticmethod
    def get_tid():
        return get_tid()

    def cpu_time(self, stat_path='/proc/stat'):
        with open(stat_path) as stat_file:
            stat_line = next(stat_file)
        return sum(float(time) for time in
                islice(stat_line.split(), 1, None)) / self.user_hz

    def tid_cpu_time(self, tid):
        stat_path = "/proc/%d/task/%d/stat" % (tid, tid)
        with open(stat_path) as stat_file:
            stat_line = next(stat_file)
        return sum(float(time) for time in
                islice(stat_line.split(), 13, 14)) / self.user_hz

    def pid_cpu_time(self, pid):
        stat_path = "/proc/%d/stat" % pid
        with open(stat_path) as stat_file:
            stat_line = next(stat_file)
        return sum(float(time) for time in
                islice(stat_line.split(), 13, 14)) / self.user_hz

    def tid_mem_total(self, tid):
        stat_path = "/proc/%d/task/%d/statm" % (tid, tid)
        with open(stat_path) as stat_file:
            stat_line = next(stat_file)
        return sum(float(time) for time in
                islice(stat_line.split(), 2, 5))

    def pid_mem_total(self, pid):
        stat_path = "/proc/%d/statm" % pid
        with open(stat_path) as stat_file:
            stat_line = next(stat_file)
        return sum(float(time) for time in
                islice(stat_line.split(), 2, 5))

    def network_route_add(self, dst=None, gw=None):
        if dst is None or gw is None:
            return
        cmd = ["ip", "route", "replace", dst, "via", gw]
        self.vcall(cmd)

    def network_bridge_add(self, name, ip):
        cmd = ["ip", "link", "show", name]
        _, _, ret = justcall(cmd)
        if ret != 0:
            cmd = ["ip", "link", "add", "name", name, "type", "bridge"]
            self.vcall(cmd)
        cmd = ["ip", "addr", "show", "dev", name]
        out, _, _ = justcall(cmd)
        if " "+ip+" " not in out:
            cmd = ["ip", "addr", "add", ip, "dev", name]
            self.vcall(cmd)

