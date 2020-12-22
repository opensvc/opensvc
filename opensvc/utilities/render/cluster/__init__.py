import os
import time
import re

import foreign.six as six
from utilities.converters import print_duration, print_size
from utilities.render.color import colorize, color, unicons
from utilities.render.listener import fmt_listener
from env import Env
from core.status import colorize_status
from utilities.naming import ANSI_ESCAPE, ANSI_ESCAPE_B, split_path, strip_path, format_path_selector, abbrev
from utilities.storage import Storage

DEFAULT_SECTIONS = [
    "threads",
    "arbitrators",
    "nodes",
    "services",
]

if six.PY2:
    pad = " "
    def print_bytes(val):
        return val+"\n"
    def bare_len(val):
        val = ANSI_ESCAPE.sub('', val)
        val = bytes(val).decode("utf-8")
        return len(val)
else:
    pad = b" "
    def print_bytes(val):
        return val.decode("utf-8")+"\n"
    def bare_len(val):
        val = ANSI_ESCAPE_B.sub(b'', val)
        val = bytes(val).decode("utf-8")
        return len(val)

def get_nodes(data):
    try:
        return data["cluster"]["nodes"]
    except:
        return [Env.nodename]

def fmt_svc_uptime(key, stats_data):
    if stats_data is None:
        return ""
    total = 0
    now = time.time()
    top = 0
    for node, _data in stats_data.items():
        try:
            uptime = now - _data["services"][key]["created"]
            if uptime > top:
                top = uptime
        except (TypeError, KeyError) as exc:
            pass
    try:
        return print_duration(top)
    except Exception:
        return ""

def fmt_svc_tasks(key, stats_data):
    if stats_data is None:
        return ""
    count = 0
    total = 0
    for _data in stats_data.values():
        try:
            total += _data["services"][key]["tasks"]
            count += 1
        except Exception:
            pass
    if not count:
        return "-"
    return str(total)

def speed(get, prev_stats, stats):
    fmt = "%8s"
    if stats is None:
        return ""
    total = 0
    for node, _data in stats.items():
        try:
            curr = get(_data)
            prev = get(prev_stats[node])
            interval = _data["timestamp"] - prev_stats[node]["timestamp"]
            total += (curr - prev) / interval
        except Exception as exc:
            raise ValueError
    if total == 0:
        return fmt % "-"
    return fmt % (print_size(total, unit="b", compact=True) + "b/s")

def fmt_svc_blk_rbps(key, prev_stats_data, stats_data):
    try:
        return speed(lambda x: x["services"][key]["blk"]["rb"], prev_stats_data, stats_data)
    except ValueError:
        return "-"

def fmt_svc_blk_wbps(key, prev_stats_data, stats_data):
    try:
        return speed(lambda x: x["services"][key]["blk"]["wb"], prev_stats_data, stats_data)
    except ValueError:
        return "-"

def cpu_usage(get, prev_stats, stats):
    try:
        node_cpu_time = stats["node"]["cpu"]["time"]
        prev_node_cpu_time = prev_stats["node"]["cpu"]["time"]
        cpu_time = get(stats)
        prev_cpu_time = get(prev_stats)
        cpu = (cpu_time - prev_cpu_time) / (node_cpu_time - prev_node_cpu_time) * 100
    except Exception as exc:
        raise ValueError
    return cpu

def fmt_thr_cpu_usage(key, prev_stats_data, stats_data):
    return fmt_cpu_usage(lambda x: x[key]["cpu"]["time"], prev_stats_data, stats_data)

def fmt_svc_cpu_usage(key, prev_stats_data, stats_data):
    return fmt_cpu_usage(lambda x: x["services"][key]["cpu"]["time"], prev_stats_data, stats_data)

def fmt_cpu_usage(get, prev_stats_data, stats_data):
    if prev_stats_data is None or stats_data is None:
        return ""
    cpu = 0
    for _node, _stats in stats_data.items():
        try:
            cpu += cpu_usage(get, prev_stats_data[_node], _stats)
        except (KeyError, ValueError) as exc:
            pass
    if cpu == 0:
        return "-"
    return "%6.1f%%" % cpu

def fmt_svc_blk_rb(key, stats_data):
    return fmt_blk(lambda x: x["services"][key]["blk"]["rb"], stats_data)

def fmt_svc_blk_wb(key, stats_data):
    return fmt_blk(lambda x: x["services"][key]["blk"]["wb"], stats_data)

def fmt_blk(get, stats_data):
    if stats_data is None:
        return ""
    val = 0
    for _data in stats_data.values():
        try:
            val += get(_data)
        except (KeyError, TypeError) as exc:
            pass
    if val == 0:
        return "     -"
    try:
        return print_size(val, unit="b", compact=True)
    except Exception:
        return "     -"

def fmt_thr_tasks(key, stats_data):
    if stats_data is None:
        return ""
    threads = 0
    procs = 0
    for _data in stats_data.values():
        if not isinstance(_data, dict):
            continue
        threads += _data.get(key, {}).get("threads", 0)
        procs += _data.get(key, {}).get("procs", 0)
    if not threads and not procs:
        return ""
    return "%d/%d" % (threads, procs)

def fmt_thr_mem_total(key, stats_data):
    return fmt_mem_total(lambda x: x[key]["mem"]["total"], stats_data)

def fmt_svc_mem_total(key, stats_data):
    return fmt_mem_total(lambda x: x["services"][key]["mem"]["total"], stats_data)

def fmt_mem_total(get, stats_data):
    if stats_data is None:
        return ""
    mem = 0
    for _data in stats_data.values():
        try:
            mem += get(_data)
        except (KeyError, TypeError) as exc:
            pass
    if mem == 0:
        return "     -"
    try:
        return print_size(mem, unit="b", compact=True)
    except Exception:
        return "     -"

def fmt_thr_cpu_time(key, stats_data):
    return fmt_cpu_time(lambda x: x[key]["cpu"]["time"], stats_data)

def fmt_svc_cpu_time(key, stats_data):
    return fmt_cpu_time(lambda x: x["services"][key]["cpu"]["time"], stats_data)

def fmt_cpu_time(get, stats_data):
    if stats_data is None:
        return ""
    time = 0
    for _data in stats_data.values():
        try:
            time += get(_data)
        except (KeyError, TypeError) as exc:
            pass
    try:
        return print_duration(time)
    except Exception:
        return ""

def fmt_tid(_data, stats_data):
    if not stats_data:
        return ""
    tid = _data.get("tid")
    if tid:
        return "%d" % tid
    return ""

def list_print(data, right=None):
    if right is None:
        right = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    outs = ""
    if len(data) == 0:
        return ""
    widths = [0] * len(data[-1])
    _data = []
    for line in data:
        _data.append(tuple(map(lambda x: x.encode("utf-8") if x is not None else "".encode("utf-8"), line)))
    for line in _data:
        for i, val in enumerate(line):
            strlen = bare_len(val)
            if strlen > widths[i]:
                widths[i] = strlen
    for line in _data:
        _line = []
        for i, val in enumerate(line):
            if widths[i] == 0:
                continue
            if i in right:
                val = pad*(widths[i]-bare_len(val)) + val
            else:
                val = val + pad*(widths[i]-bare_len(val))
            _line.append(val)
        _line = pad.join(_line)
        outs += print_bytes(_line)
    return outs

def print_section(data):
    if len(data) == 0:
        return ""
    return list_print(data)


def format_cluster(paths=None, node=None, data=None, prev_stats_data=None,
                   stats_data=None, sections=None, selector=None,
                   namespace=None):
    if not data or data.get("status", 0) != 0:
        return
    if sections is None:
        sections = DEFAULT_SECTIONS
    out = []
    avail_nodenames = get_nodes(data)
    nodenames = sorted([n for n in avail_nodenames if n in node])
    show_nodenames = abbrev(nodenames)
    services = {}

    def load_header(title=""):
        if isinstance(title, list):
            line = title
        else:
            line = [
                title,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        for nodename in show_nodenames:
            line.append(colorize(nodename, color.BOLD))
        out.append(line)

    def load_svc(path, prefix=""):
        if path not in services:
            return
        data = services[path]
        if path in slave_parents and prefix == "":
            return
        try:
            topology = services[path].topology
        except KeyError:
            topology = ""
        if services[path].get("drp", False):
            topology = "drp " + topology

        # status
        status = colorize_status(data["avail"], lpad=0)
        if data["overall"] == "warn":
            status += colorize("!", color.BROWN)
        if data["placement"] == "non-optimal":
            status += colorize("^", color.RED)

        # info
        info = {
            "topology": data.get("topology", ""),
            "orchestrate": data.get("orchestrate", "-"),
            "status": "%d/1" % data["n_up"] if data["n_up"] is not None else 0,
        }
        if data.get("scale") is not None:
            info["status"] = "%d/%d" % (data["n_up"], data.get("scale"))
        elif data.get("wrapper"):
            info = {
                "topology": "",
                "orchestrate": "",
                "status": "",
            }
        elif topology == "flex":
            info["status"] = "%d/%d" % (data["n_up"], data["flex_target"])
        if data["avail"] == "n/a":
            info["status"] = ""
        info = "%(orchestrate)-5s %(status)-5s" % info
        line = [
            " "+colorize(prefix+strip_path(path, os.environ.get("OSVC_NAMESPACE")), color.BOLD),
            status,
            info,
            fmt_svc_uptime(path, stats_data),
            fmt_svc_tasks(path, prev_stats_data),
            fmt_svc_cpu_usage(path, prev_stats_data, stats_data),
            fmt_svc_cpu_time(path, stats_data),
            fmt_svc_mem_total(path, stats_data),
            fmt_svc_blk_rb(path, stats_data),
            fmt_svc_blk_wb(path, stats_data),
            fmt_svc_blk_rbps(path, prev_stats_data, stats_data),
            fmt_svc_blk_wbps(path, prev_stats_data, stats_data),
            "|" if nodenames else "",
        ]
        if not nodenames:
            states = []
            if data["frozen"] == "frozen":
                frozen = "frozen"
            elif data["frozen"] == "thawed":
                frozen = ""
            elif data["frozen"] == "n/a":
                frozen = ""
            elif data["frozen"] == "mixed":
                frozen = "part-frozen"
            else:
                frozen = ""
            if frozen:
                states.append(frozen)

            if data["provisioned"] is True:
                provisioned = ""
            elif data["provisioned"] is False:
                provisioned = "unprovisioned"
            elif data["provisioned"] == "n/a":
                provisioned = ""
            elif data["provisioned"] == "mixed":
                provisioned = "part-provisioned"
            else:
                provisioned = ""
            if provisioned:
                states.append(provisioned)

            mon_status_counts = {}
            ge = None
            for nodename in avail_nodenames:
                try:
                    _data = data["nodes"][nodename]
                except KeyError:
                    continue
                if _data is None:
                    continue
                ge = _data.get("global_expect")
                st = _data.get("mon")
                if st not in ("idle", None):
                    if st not in mon_status_counts:
                        mon_status_counts[st] = 1
                    else:
                        mon_status_counts[st] += 1
            if ge:
                states.append(">"+ge)
            for s, n in mon_status_counts.items():
                states.append("%s(%d)" % (s, n))
            line.append(", ".join(states))

        for nodename in nodenames:
            if nodename not in data["nodes"]:
                line.append("")
            elif data["nodes"][nodename] is None:
                line.append(colorize("?", color.RED))
            elif data["nodes"][nodename] is not None:
                val = []
                # frozen unicon
                if data["nodes"][nodename]["frozen"]:
                    frozen_icon = colorize(unicons["frozen"], color.BLUE)
                else:
                    frozen_icon = ""
                # avail status unicon
                if data["wrapper"]:
                    avail_icon = ""
                else:
                    avail = data["nodes"][nodename]["avail"]
                    if avail == "unknown":
                        avail_icon = colorize("?", color.RED)
                    else:
                        avail_icon = colorize_status(avail, lpad=0, agg_status=data["avail"]).replace(avail, unicons[avail])
                    if data["nodes"][nodename].get("preserved"):
                        avail_icon += colorize("?", color.LIGHTBLUE)
                # overall status unicon
                if data["wrapper"]:
                    overall_icon = ""
                else:
                    overall = data["nodes"][nodename]["overall"]
                    if overall == "warn":
                        overall_icon = colorize_status(overall, lpad=0).replace(overall, unicons[overall])
                    else:
                        overall_icon = ""
                # mon status
                smon = data["nodes"][nodename]["mon"]
                if smon == "idle":
                    # don't display 'idle', as its to normal status and thus repeated as nauseam
                    smon = ""
                else:
                    smon = " " + smon
                # global expect
                if smon == "":
                    global_expect = data["nodes"][nodename]["global_expect"]
                    if global_expect:
                        global_expect = colorize(" >" + str(global_expect), color.LIGHTBLUE)
                    else:
                        global_expect = ""
                else:
                    global_expect = ""
                # leader
                if data["wrapper"]:
                    leader = ""
                else:
                    if data["nodes"][nodename]["placement"] == "leader":
                        leader = colorize("^", color.LIGHTBLUE)
                    else:
                        leader = ""
                # provisioned
                if data["nodes"][nodename].get("provisioned") is False:
                    provisioned = colorize("P", color.RED)
                else:
                    provisioned = ""

                val.append(avail_icon)
                val.append(overall_icon)
                val.append(leader)
                val.append(frozen_icon)
                val.append(provisioned)
                val.append(smon)
                val.append(global_expect)
                line.append("".join(val))
        out.append(line)

        for child in sorted(list(data.get("slaves", []))):
            load_svc(child, prefix=prefix+" ")

    def load_hb(key, _data):
        state = _data.get("state", "")
        if state == "running":
            state = colorize(state, color.GREEN)
        else:
            state = colorize(state, color.RED)
        if _data.get("alerts"):
            state += colorize("!", color.BROWN)
        cf = _data.get("config", {})
        addr = cf.get("addr", "")
        port = cf.get("port", "")
        dev = cf.get("dev", "")
        relay = cf.get("relay", "")
        if addr and port:
            config = fmt_listener(addr, port)
        elif dev:
            config = os.path.basename(dev)
        elif relay:
            config = relay
        else:
            config = ""
        line = [
            " "+colorize(key, color.BOLD),
            state,
            config,
            fmt_tid(_data, stats_data),
            fmt_thr_tasks(key, stats_data),
            fmt_thr_cpu_usage(key, prev_stats_data, stats_data),
            fmt_thr_cpu_time(key, stats_data),
            "",
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        peers = _data.get("peers", {})
        for nodename in nodenames:
            beating = peers.get(nodename, {}).get("beating")
            if beating is None:
                status = "n/a"
            elif beating:
                status = "up"
            else:
                status = "down"
            status = colorize_status(status, lpad=0).replace(status, unicons[status])
            line.append(status)
        out.append(line)

    def load_monitor(key, _data):
        if "state" not in _data:
            _data["state"] = "undef"
        if _data["state"] == "running":
            state = colorize(_data["state"], color.GREEN)
        else:
            state = colorize(_data["state"], color.RED)
        transitions = _data.get("transitions", 0)
        if transitions:
            status = "%d transition" % transitions
        else:
            status = ""
        out.append((
            " "+colorize(key, color.BOLD),
            state,
            status,
            fmt_tid(_data, stats_data),
            fmt_thr_tasks(key, stats_data),
            fmt_thr_cpu_usage(key, prev_stats_data, stats_data),
            fmt_thr_cpu_time(key, stats_data),
            "",
            "",
            "",
            "",
            "",
        ))

    def load_listener(key, _data):
        if _data["state"] == "running":
            state = colorize(_data["state"], color.GREEN)
        else:
            state = colorize(_data["state"], color.RED)
        out.append((
            " "+colorize(key, color.BOLD),
            state,
            fmt_listener(_data["config"]["addr"], _data["config"]["port"]),
            fmt_tid(_data, stats_data),
            fmt_thr_tasks(key, stats_data),
            fmt_thr_cpu_usage(key, prev_stats_data, stats_data),
            fmt_thr_cpu_time(key, stats_data),
            "",
            "",
            "",
            "",
            "",
        ))

    def load_scheduler(key, _data):
        if _data["state"] == "running":
            state = colorize(_data["state"], color.GREEN)
        else:
            state = colorize(_data["state"], color.RED)
        out.append((
            " "+colorize(key, color.BOLD),
            state,
            "",
            fmt_tid(_data, stats_data),
            fmt_thr_tasks(key, stats_data),
            fmt_thr_cpu_usage(key, prev_stats_data, stats_data),
            fmt_thr_cpu_time(key, stats_data),
            "",
            "",
            "",
            "",
            "",
        ))

    def load_daemon():
        key = "daemon"
        state = colorize("running", color.GREEN)
        line = [
            " "+colorize(key, color.BOLD),
            "%s" % state,
            "",
            str(data.get("pid", "")) if stats_data else "",
            "",
            fmt_thr_cpu_usage(key, prev_stats_data, stats_data),
            fmt_thr_cpu_time(key, stats_data),
            fmt_thr_mem_total(key, stats_data),
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        for nodename in nodenames:
            speaker = data["monitor"].get("nodes", {}).get(nodename, {}).get("speaker")
            if speaker:
                status = "up"
                status = colorize_status(status, lpad=0).replace(status, unicons[status])
            else:
                status = ""
            line.append(status)
        out.append(line)


    def load_collector(key, _data):
        if _data["state"] == "running":
            state = colorize(_data["state"], color.GREEN)
        else:
            state = colorize(_data["state"], color.RED)
        line = [
            " "+colorize(key, color.BOLD),
            state,
            "",
            fmt_tid(_data, stats_data),
            fmt_thr_tasks(key, stats_data),
            fmt_thr_cpu_usage(key, prev_stats_data, stats_data),
            fmt_thr_cpu_time(key, stats_data),
            "",
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        for nodename in nodenames:
            speaker = data["monitor"].get("nodes", {}).get(nodename, {}).get("speaker")
            if speaker:
                status = "up"
                status = colorize_status(status, lpad=0).replace(status, unicons[status])
            else:
                status = ""
            line.append(status)
        out.append(line)

    def load_generic_thread(key, _data):
        if _data["state"] == "running":
            state = colorize(_data["state"], color.GREEN)
        else:
            state = colorize(_data["state"], color.RED)
        out.append((
            " "+colorize(key, color.BOLD),
            state,
            "",
            fmt_tid(_data, stats_data),
            fmt_thr_tasks(key, stats_data),
            fmt_thr_cpu_usage(key, prev_stats_data, stats_data),
            fmt_thr_cpu_time(key, stats_data),
            "",
            "",
            "",
        ))

    def load_threads():
        if "threads" not in sections:
            return
        load_header([
            "Threads",
            "",
            "",
            "pid/tid" if stats_data else "",
            "thr/sub" if stats_data else "",
            "usage" if stats_data else "",
            "time" if stats_data else "",
            "rss" if stats_data else "",
            "",
            "",
            "",
            "",
            "",
        ])
        load_daemon()
        for key in sorted([key for key in data if key != "cluster"]):
            if key.startswith("hb#"):
                load_hb(key, data[key])
            elif key == "monitor":
                load_monitor(key, data[key])
            elif key == "scheduler":
                load_scheduler(key, data[key])
            elif key == "listener":
                load_listener(key, data[key])
            elif key == "collector":
                load_collector(key, data[key])
            else:
                try:
                    load_generic_thread(key, data[key])
                except Exception:
                    pass
        out.append([])

    def load_score():
        if "monitor" not in data:
            return
        line = [
            colorize(" score", color.BOLD),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        for nodename in nodenames:
            line.append(str(data["monitor"]["nodes"].get(nodename, {}).get("stats", {}).get("score", "")))
        out.append(line)

    def load_loadavg():
        if "monitor" not in data:
            return
        line = [
            colorize("  load 15m", color.BOLD),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        for nodename in nodenames:
            line.append(str(data["monitor"]["nodes"].get(nodename, {}).get("stats", {}).get("load_15m", "")))
        out.append(line)

    def load_free_total(key):
        if "monitor" not in data:
            return
        line = [
            colorize("  "+key, color.BOLD),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        for nodename in nodenames:
            total = data["monitor"]["nodes"].get(nodename, {}).get("stats", {}).get(key+"_total")
            avail = data["monitor"]["nodes"].get(nodename, {}).get("stats", {}).get(key+"_avail")
            limit = 100 - data["monitor"]["nodes"].get(nodename, {}).get("min_avail_"+key, 0)
            if avail is None or total in (0, None):
                line.append(colorize("-", color.LIGHTBLUE))
                continue
            usage = 100 - avail
            total = print_size(total, unit="MB", compact=True)
            if limit:
                cell = "%d/%d%%:%s" % (usage, limit, total)
            else:
                cell = "%d%%:%s" % (usage, total)
            if usage > limit:
                cell = colorize(cell, color.RED)
            line.append(cell)
        out.append(line)

    def load_node_state():
        if "monitor" not in data:
            return
        line = [
            colorize(" state", color.BOLD),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        for nodename in nodenames:
            nmon_state = data["monitor"]["nodes"].get(nodename, {}).get("monitor", {}).get("status", "")
            if nmon_state == "idle":
                nmon_state = ""
            if data["monitor"]["nodes"].get(nodename, {}).get("frozen", ""):
                frozen = frozen_icon = colorize(unicons["frozen"], color.BLUE)
            else:
                frozen = ""
            global_expect = data["monitor"]["nodes"].get(nodename, {}).get("monitor", {}).get("global_expect")
            if global_expect:
                global_expect = colorize(" >" + str(global_expect), color.LIGHTBLUE)
            else:
                global_expect = ""
            line.append(str(nmon_state)+frozen+global_expect)
        out.append(line)

    def load_node_compat():
        if "monitor" not in data:
            return
        if data["monitor"].get("compat") is True:
            # no need to clutter if the situation is normal
            return
        line = [
            colorize(" compat", color.BOLD),
            colorize("warn", color.BROWN),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        for nodename in nodenames:
            compat = data["monitor"]["nodes"].get(nodename, {}).get("compat", "")
            line.append(str(compat))
        out.append(line)

    def load_node_version():
        if "monitor" not in data:
            return
        line = [
            colorize(" version", color.BOLD),
            colorize("warn", color.BROWN),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "|" if nodenames else "",
        ]
        versions = []
        for nodename in nodenames:
            agent = data["monitor"]["nodes"].get(nodename, {}).get("agent", "")
            line.append(str(agent))
            if agent != "":
                versions.append(str(agent))
        if len(set(versions)) > 1:
            out.append(line)

    def load_arbitrators():
        if "arbitrators" not in sections:
            return
        arbitrators = []
        arbitrators_name = {}
        for nodename, ndata in data["monitor"]["nodes"].items():
            for aid, adata in ndata.get("arbitrators", {}).items():
                 if aid not in arbitrators:
                     arbitrators.append(aid)
                     arbitrators_name[aid] = adata["name"]
        if len(arbitrators) == 0:
            return
        load_header("Arbitrators")
        for aid in arbitrators:
            line = [
                colorize(" "+arbitrators_name[aid], color.BOLD),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "|" if nodenames else "",
            ]
            for nodename in nodenames:
                status = data["monitor"]["nodes"].get(nodename, {}).get("arbitrators", {}).get(aid, {}).get("status", "undef")
                if status != "up":
                    line[1] = colorize_status("warn", lpad=0)
                status = colorize_status(status, lpad=0).replace(status, unicons[status])
                line.append(status)
            out.append(line)
        out.append([])

    def load_nodes():
        if "nodes" not in sections or not nodenames:
            return
        load_header("Nodes")
        load_metrics()
        load_node_compat()
        load_node_version()
        load_node_state()
        out.append([])

    def load_metrics():
        load_score()
        load_loadavg()
        load_free_total("mem")
        load_free_total("swap")

    # init the services hash
    slave_parents = {}
    if "monitor" in data:
        for _node in avail_nodenames:
            if _node not in data["monitor"]["nodes"]:
                continue
            try:
                node_svc_status = data["monitor"]["nodes"][_node]["services"]["status"]
            except KeyError:
                continue
            for path, _data in node_svc_status.items():
                if _data is None:
                    continue
                if paths is not None and path not in paths:
                    continue
                if path not in services:
                    services[path] = Storage({
                        "drp": _data.get("drp", False),
                        "topology": _data.get("topology", ""),
                        "orchestrate": _data.get("orchestrate", ""),
                        "flex_target": _data.get("flex_target"),
                        "scale": _data.get("scale"),
                        "avail": "undef",
                        "overall": "",
                        "nodes": {},
                        "slaves": set(),
                        "n_up": 0,
                        "resources": set(),
                    })
                try:
                    services[path]["resources"] |= set(_data["resources"].keys())
                except KeyError:
                    pass
                slaves = _data.get("slaves", [])
                scale = _data.get("scale")
                if scale:
                    name, _namespace, kind = split_path(path)
                    if _namespace:
                        pattern = r"^%s/%s/[0-9]+\.%s$" % (_namespace, kind, name)
                    else:
                        pattern = r"^[0-9]+\.%s$" % name
                    for child in data["monitor"]["services"]:
                        if re.match(pattern, child) is None:
                            continue
                        slaves.append(child)
                        if node_svc_status.get(child, {}).get("avail") == "up":
                            services[path].n_up += 1
                else:
                    if node_svc_status.get(path, {}).get("avail") == "up":
                        services[path].n_up += 1
                for child in slaves:
                    if child not in slave_parents:
                        slave_parents[child] = set([path])
                    else:
                        slave_parents[child] |= set([path])
                global_expect = _data.get("monitor", {}).get("global_expect")
                if global_expect and "@" in global_expect:
                    global_expect = global_expect[:global_expect.index("@")+1]
                services[path].nodes[_node] = {
                    "avail": _data.get("avail", "undef"),
                    "preserved": _data.get("preserved"),
                    "overall": _data.get("overall", "undef"),
                    "frozen": _data.get("frozen", False),
                    "mon": _data.get("monitor", {}).get("status", ""),
                    "global_expect": global_expect,
                    "placement": _data.get("monitor", {}).get("placement", ""),
                    "provisioned": _data.get("provisioned"),
                }
                services[path].slaves |= set(slaves)
                services[path]["wrapper"] = (
                    services[path].resources == set() and
                    services[path].slaves != set() and
                    scale is None
                )
            try:
                # hint we have missing instances
                for path, cnf in data["monitor"]["nodes"][_node]["services"]["config"].items():
                    if path not in services:
                        continue
                    for __node in cnf.get("scope", []):
                        if __node not in services[path].nodes:
                            services[path].nodes[__node] = None
            except KeyError:
                pass
        for path, _data in data["monitor"]["services"].items():
            if paths is not None and path not in paths:
                continue
            if path not in services:
                services[path] = Storage({
                    "avail": "undef",
                    "overall": "",
                    "nodes": {}
                })
            services[path].avail = _data.get("avail", "n/a")
            services[path].overall = _data.get("overall", "n/a")
            services[path].placement = _data.get("placement", "n/a")
            services[path].frozen = _data.get("frozen", "n/a")
            services[path].provisioned = _data.get("provisioned", "n/a")

    def load_services(selector, namespace=None):
        if "services" not in sections:
            return
        selectors = []
        context = os.environ.get("OSVC_CONTEXT", "")
        if context:
            selectors.append(context)
        buff = format_path_selector(selector, namespace, maxlen=15)
        selectors.append(buff)
        header = [
            "/".join(selectors),
            "",
            "",
            "since" if stats_data else "",
            "tasks" if stats_data else "",
            "usage" if stats_data else "",
            "time" if stats_data else "",
            "mem" if stats_data else "",
            "blkrb" if stats_data else "",
            "blkwb" if stats_data else "",
            "blkrbps" if stats_data else "",
            "blkwbps" if stats_data else "",
            "",
        ]
        if not nodenames:
            header.append("")
        load_header(header)
        for path in sorted(list(services.keys())):
            load_svc(path)

    # load data in lists
    load_threads()
    load_arbitrators()
    load_nodes()
    load_services(selector, namespace)

    # print tabulated lists
    return print_section(out)


