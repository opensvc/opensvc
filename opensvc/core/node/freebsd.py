from utilities.proc import justcall

from .node import Node as BaseNode


class Node(BaseNode):
    def shutdown(self):
        cmd = ["halt"]
        ret, out, err = self.vcall(cmd)

    def _reboot(self):
        cmd = ["reboot"]
        ret, out, err = self.vcall(cmd)

    def stats_meminfo(self):
        """
        Memory sizes are store in MB.
        Avails are percentages.
        """
        cmd = [
            "sysctl",
            "hw.realmem",
            "hw.pagesize",
            "vm.stats.vm.v_page_count",
            "vm.stats.vm.v_wire_count",
            "vm.stats.vm.v_active_count",
            "vm.stats.vm.v_inactive_count",
            "vm.stats.vm.v_cache_count",
            "vm.stats.vm.v_free_count",
            "vm.swap_total",
        ]
        raw_data = {}
        data = {}
        out, err, ret = justcall(cmd)
        for line in out.splitlines():
            key, val = line.split(":", 1)
            val = val.strip()
            raw_data[key] = int(val)

        data["mem_total"] = raw_data["hw.realmem"] // 1024 // 1024
        data["mem_avail"] = (raw_data["vm.stats.vm.v_inactive_count"] + raw_data["vm.stats.vm.v_cache_count"] + raw_data["vm.stats.vm.v_free_count"]) * raw_data["hw.pagesize"] // 1024 // 1024
        if data["mem_total"]:
            data["mem_avail"] = 100 * data["mem_avail"] // data["mem_total"]
        else:
            data["mem_avail"] = 0

        data["swap_total"] = raw_data["vm.swap_total"] // 1024 // 1024

        cmd = ["pstat", "-T"]
        out, err, ret = justcall(cmd)
        for line in out.splitlines():
            if "swap" in line:
                swap_used = int(line.split("M/")[0])
                break
        if data["swap_total"]:
            swap_avail = data["swap_total"] - swap_used
            data["swap_avail"] = 100 * swap_avail // data["swap_total"]
        else:
            data["swap_avail"] = 0

        return data

if __name__ == "__main__":
    print(Node().stats_meminfo())
