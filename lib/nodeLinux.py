from itertools import islice
from rcUtilitiesLinux import get_tid
import node

class Node(node.Node):
    def shutdown(self):
        cmd = ["shutdown", "-h"]
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

    @staticmethod
    def get_tid():
        return get_tid()

    @staticmethod
    def cpu_time(stat_path='/proc/stat'):
        with open(stat_path) as stat_file:
            cpu_stat_line = next(stat_file)
        return sum(float(time) for time in
                islice(cpu_stat_line.split(), 1, 9))

    @staticmethod
    def pid_cpu_time(pid):
        stat_path = "/proc/%d/stat" % pid
        with open(stat_path) as stat_file:
            cpu_stat_line = next(stat_file)
        return sum(float(time) for time in
                islice(cpu_stat_line.split(), 13, 14))
