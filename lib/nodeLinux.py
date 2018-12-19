import os
from itertools import islice
from rcUtilitiesLinux import get_tid
from rcUtilities import lazy
import node

class Node(node.Node):
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
