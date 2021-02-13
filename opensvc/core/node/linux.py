import os
import time
from itertools import islice

import utilities.os.linux
from utilities.lazy import lazy
from utilities.proc import justcall

from .node import Node as BaseNode


class Node(BaseNode):
    def still_alive(self, action):
        try:
            with open("/proc/sys/kernel/sysrq", "r") as ofile:
                buff = ofile.read()
        except Exception:
            buff = "<unknown>"
        self.log.error("still alive ... maybe %s is ignored by kernel.sysrq=%s "
                       "(check dmesg)" % (action, buff.strip()))
        time.sleep(1)
        self.freeze()

    def sys_reboot(self, delay=0):
        if delay:
            self.log.info("sysrq reboot in %s seconds", delay)
            time.sleep(delay)
        with open("/proc/sysrq-trigger", "w") as ofile:
            ofile.write("b")
        self.still_alive("reboot")

    def sys_crash(self, delay=0):
        if delay:
            self.log.info("sysrq crash in %s seconds", delay)
            time.sleep(delay)
        with open("/proc/sysrq-trigger", "w") as ofile:
            ofile.write("c")
        self.still_alive("crash")

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
        data["mem_avail"] = 100 * (raw_data["MemFree"] + raw_data["Cached"] + raw_data.get("SwapCached", 0) + raw_data.get("SReclaimable", 0)) // raw_data["MemTotal"]
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
        return utilities.os.linux.get_tid()

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

    def network_route_add(self, dst=None, gw=None, dev=None, local_ip=None, brdev=None, brip=None, table=None, tunnel="auto", **kwargs):
        if dst is None:
            return
        if tunnel in ("auto", "never"):
            if gw is not None:
                cmd = ["ip", "route", "replace", dst, "via", gw, "table", table]
            elif dev is not None:
                cmd = ["ip", "route", "replace", dst, "dev", dev, "table", table]
            out, err, ret = justcall(cmd)
        else:
            err = ""
        if tunnel == "never":
            self.log.info(" ".join(cmd))
            for line in out.splitlines():
                self.log.info(line)
        elif tunnel == "always" or "invalid gateway" in err or "is unreachable" in err:
            tun = self.network_tunnel_ipip_add(local_ip, gw)
            cmd = ["ip", "route", "replace", dst, "dev", tun["dev"], "src", brip.split("/")[0], "table", table]
            self.vcall(cmd)
        else:
            self.log.info(" ".join(cmd))
            for line in out.splitlines():
                self.log.info(line)

    def mac_from_ip(self, ip):
        mac = "0a:58"
        for i in ip.split("/", 1)[0].split("."):
            mac += ":%.2x" % int(i)
        return mac

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
        cmd = ["ip", "link", "show", "dev", name]
        out, _, _ = justcall(cmd)
        mac = self.mac_from_ip(ip)
        if mac not in out:
            cmd = ["ip", "link", "set", "dev", name, "address", self.mac_from_ip(ip)]
            self.vcall(cmd)
        if "DOWN" in out:
            cmd = ["ip", "link", "set", "dev", name, "up"]
            self.vcall(cmd)

    def network_ip_intf(self, addr):
        cmd = ["ip", "addr"]
        out, _, _ = justcall(cmd)
        marker = "inet %s/" % addr
        for line in out.splitlines():
            if marker in line:
                return line.split()[-1]

    def network_tunnel_ipip_add(self, src, dst):
        src_dev = self.network_ip_intf(src)
        name = "tun" + dst.split("/", 1)[0].replace(".", "")
        cmd = ["ip", "tunnel", "show", name]
        out, err, ret = justcall(cmd)
        if out:
            action = "change"
        else:
            action = "add"
        cmd = ["ip", "tunnel", action, name, "mode", "ipip",
               "local", src, "remote", dst]
        if src_dev:
            cmd += ["dev", src_dev]
        self.vcall(cmd)
        if action == "add":
            cmd = ["ip", "link", "set", "dev", name, "up"]
            self.vcall(cmd)
        return {
            "dev": name,
            "local": src,
            "remote": dst,
        }

    def network_create_fwrules(self, name):
        nets = self.networks_data()
        data = nets[name]
        ntype = data["config"]["type"]
        if ntype not in ("bridge", "routed_bridge"):
            return
        chain = "osvc-" + name
        comment = "name: %s" % name
        self.network_ipt_add_chain(chain, nat=True)
        for net in nets.values():
            if net["config"]["network"] == "undef":
                continue
            self.network_ipt_add_rule(chain, nat=True, dst=net["config"]["network"], act="RETURN", comment=comment, where="head")
        self.network_ipt_add_rule(chain=chain, nat=True, dst="!224.0.0.0/4", act="MASQUERADE", comment=comment, where="tail")
        src = data.get("cni", {}).get("data", {}).get("ipam", {}).get("subnet")
        if not src:
            src = data.get("cni", {}).get("data", {}).get("network")
        self.network_ipt_add_rule(chain="POSTROUTING", nat=True, src=src, act=chain, comment=comment)
        self.network_ipt_add_rule(chain="FORWARD", nat=False, indev="obr_"+name, act="ACCEPT", comment=comment)
        self.network_ipt_add_rule(chain="FORWARD", nat=False, outdev="obr_"+name, act="ACCEPT", comment=comment)

    def network_ipt_add_rule(self, chain=None, nat=False, dst=None, src=None, act="RETURN", comment=None, where="tail", indev=None, outdev=None):
        if nat:
            nat = ["-t", "nat"]
        else:
            nat = []
        if where == "head":
            where = "-I"
        else:
            where = "-A"
        cmd1 = ["iptables"] + nat
        cmd2 = [chain]
        if indev:
            cmd2 += ["-i", indev]
        if outdev:
            cmd2 += ["-o", outdev]
        if src:
            if src[0] == "!":
                cmd2 += ["!"]
                src = src[1:]
            cmd2 += ["-s", src]
        if dst:
            if dst[0] == "!":
                cmd2 += ["!"]
                dst = dst[1:]
            cmd2 += ["-d", dst]
        cmd2 += ["-j", act]
        if comment:
            cmd2 += ["-m", "comment", "--comment", comment]
        cmd = cmd1 + ["-C"] + cmd2
        out, err, ret = justcall(cmd)
        if ret == 0:
            return
        cmd = cmd1 + [where] + cmd2
        self.log.info(" ".join(cmd))
        out, err, ret = justcall(cmd)
        if ret != 0 and err:
            self.log.error(err)

    def network_ipt_add_chain(self, chain, nat=False):
        if nat:
            nat = ["-t", "nat"]
        else:
            nat = []
        cmd = ["iptables"] + nat + ["-N", chain]
        out, err, ret = justcall(cmd)
        if "already exist" in err:
            return
        self.log.info(" ".join(cmd))
        if ret != 0:
            self.log.error(err)
