import re
import os

import utilities.render.forest

from env import Env
from utilities.naming import split_path, strip_path, resolve_path, is_service
from utilities.render.color import color, colorize, STATUS_COLOR
from utilities.storage import Storage
from core.objects.svc import DEFAULT_STATUS_GROUPS

def fmt_flags(resource, idata):
    """
    Format resource flags as a vector of character.

    R  Running
    M  Monitored
    D  Disabled
    O  Optional
    E  Encap
    P  Provisioned
    S  Standby
    """
    provisioned = resource.get("provisioned", {}).get("state")

    rid = resource.get("rid")
    restart = resource.get("restart", 0)
    retries = idata.get("monitor", {}).get("restart", {}).get(rid, 0)
    if not isinstance(restart, int) or restart < 1:
        restart_flag = "."
    else:
        remaining_restart = restart - retries
        if remaining_restart < 0:
            remaining_restart = 0
        if remaining_restart < 10:
            restart_flag = str(remaining_restart)
        else:
            restart_flag = "+"

    flags = ""
    flags += "R" if rid in idata.get("running", []) else "."
    flags += "M" if resource.get("monitor") else "."
    flags += "D" if resource.get("disable") else "."
    flags += "O" if resource.get("optional") else "."
    flags += "E" if resource.get("encap") else "."
    flags += "." if provisioned else "P" if provisioned is False else "/"
    flags += 'S' if resource.get("standby") else '.'
    flags += restart_flag
    return flags

def dispatch_resources(idata, discard_disabled=False):
    """
    Sorted resources.
    Honor the --discard-disabled arg.
    """
    subsets = {}
    for group in DEFAULT_STATUS_GROUPS:
        subsets[group] = {}

    for rid, resource in idata.get("resources", {}).items():
        if discard_disabled and resource.get("disable", False):
            continue
        group = resource["type"].split(".", 1)[0]
        if "subset" in resource:
            subset = group + ":" + resource["subset"]
        else:
            subset = group
        if group not in subsets:
            subsets[group] = {}
        if subset not in subsets[group]:
            subsets[group][subset] = []
        resource["rid"] = rid
        subsets[group][subset].append(resource)

    return subsets

def add_subsets(subsets, node_nodename, idata, discard_disabled=False):
    done = set()
    def do(group):
        try:
            subset_names = sorted(subsets[group])
        except KeyError:
            return
        for subset in subset_names:
            if subset != group:
                node_subset = node_nodename.add_node()
                node_subset.add_column(subset)
                parallel = idata.get("subsets", {}).get(subset, {}).get("parallel", False)
                if parallel:
                    node_subset.add_column()
                    node_subset.add_column()
                    node_subset.add_column("//")
            else:
                node_subset = node_nodename
            for resource in sorted(subsets[group][subset], key=lambda x: x["rid"]):
                add_res_node(resource, node_subset, idata, discard_disabled=discard_disabled)
    for group in DEFAULT_STATUS_GROUPS:
        done.add(group)
        do(group)

    # data resources
    left = sorted(list(set(subsets.keys()) - done))
    for group in left:
        do(group)

def get_svc_notice(data):
    svc_notice = []
    if "cluster" in data:
        overall = data["cluster"].get("overall", "n/a")
        if overall == "warn":
            svc_notice.append(colorize(overall, STATUS_COLOR[overall]))
        placement = data["cluster"].get("placement", "n/a")
        if placement not in ("optimal", "n/a"):
            svc_notice.append(colorize(placement + " placement", color.RED))
        compat = data["cluster"].get("compat", True)
        if not compat:
            svc_notice.append(colorize("incompatible versions", color.RED))
    return ", ".join(svc_notice)

def instance_notice(overall=None, frozen=None, node_frozen=None, constraints=None,
                    provisioned=None, monitor=None, priority=Env.default_priority):
    notice = []
    if overall == "warn":
        notice.append(colorize(overall, STATUS_COLOR[overall]))
    if frozen:
        notice.append(colorize("frozen", color.BLUE))
    if node_frozen:
        notice.append(colorize("node frozen", color.BLUE))
    if not constraints:
        notice.append("constraints violation")
    if provisioned is False:
        notice.append(colorize("not provisioned", color.RED))
    if priority != Env.default_priority:
        notice.append(colorize("p%d" % priority, color.LIGHTBLUE))
    if monitor == {}:
        # encap monitor
        pass
    elif monitor:
        mon_status = monitor.get("status", "unknown")
        if monitor["status"] == "idle":
            notice.append(colorize(mon_status, color.LIGHTBLUE))
        else:
            notice.append(colorize(mon_status, color.RED))
        if monitor.get("local_expect") not in ("", None):
            notice.append(colorize(monitor.get("local_expect", ""), color.LIGHTBLUE))
        if monitor.get("global_expect") not in ("", None):
            notice.append(colorize(">"+monitor.get("global_expect", ""), color.LIGHTBLUE))
    else:
        notice.append(colorize("daemon down", color.RED))
    return ", ".join(notice)

def add_res_node(resource, parent, idata, rid=None, discard_disabled=False):
    if discard_disabled and resource.get("disable", False):
        return
    ers = get_ers(idata)
    rid = resource["rid"]
    node_res = parent.add_node()
    node_res.add_column(rid)
    node_res.add_column(fmt_flags(resource, idata))
    node_res.add_column(resource["status"],
                        STATUS_COLOR[resource["status"]])
    col = node_res.add_column(resource["label"])
    if rid in ers and resource["status"] in ("up", "stdby up", "n/a"):
        edata = Storage(idata["encap"].get(rid))
        encap_notice = instance_notice(
            overall=edata.overall,
            frozen=edata.frozen,
            constraints=edata.get("constraints", True),
            provisioned=edata.provisioned,
            monitor={},
            priority=edata.get("priority", Env.default_priority),
        )
        col.add_text(encap_notice, color.LIGHTBLUE)
    for line in resource.get("log", []):
        if line.startswith("warn:"):
            scolor = STATUS_COLOR["warn"]
        elif line.startswith("error:"):
            scolor = STATUS_COLOR["err"]
        else:
            scolor = None
        col.add_text(line, scolor)

    if rid not in ers or resource["status"] not in ("up", "stdby up", "n/a"):
        return

    add_subsets(ers[rid], node_res, idata, discard_disabled=discard_disabled)

def get_scope(path, mon_data):
    nodes = []
    for ndata in mon_data.get("nodes", {}).values():
        try:
            nodes = ndata["services"]["config"][path]["scope"]
            break
        except KeyError:
            pass
    return sorted(nodes)

def add_instances(node, path, ref_nodename, mon_data):
    for nodename in get_scope(path, mon_data):
        if nodename == ref_nodename:
            continue
        add_instance(node, nodename, path, mon_data)

def add_instance(node, nodename, path, mon_data):
    node_child = node.add_node()
    node_child.add_column(nodename, color.BOLD)
    node_child.add_column()
    try:
        data = mon_data["nodes"][nodename]["services"]["status"][path]
        avail = data.get("avail", "n/a")
        node_frozen = mon_data["nodes"][nodename].get("frozen")
    except (TypeError, KeyError) as exc:
        avail = "undef"
        node_frozen = False
        data = Storage()
    node_child.add_column(avail, STATUS_COLOR[avail])
    notice = instance_notice(
        overall=data.get("overall", "n/a"),
        frozen=data.get("frozen"),
        node_frozen=node_frozen,
        constraints=data.get("constraints", True),
        provisioned=data.get("provisioned"),
        monitor=data.get("monitor"),
        priority=data.get("priority", Env.default_priority),
    )
    node_child.add_column(notice, color.LIGHTBLUE)

def add_parents(node, idata, mon_data, namespace):
    parents = idata.get("parents", [])
    if len(parents) == 0:
        return
    node_parents = node.add_node()
    node_parents.add_column("parents")
    for parent in parents:
        add_parent(parent, node_parents, mon_data, namespace)

def add_parent(path, node, mon_data, namespace):
    node_parent = node.add_node()
    try:
        path, nodename = path.split("@")
        path = resolve_path(path, namespace)
        node_parent.add_column(strip_path(path, os.environ.get("OSVC_NAMESPACE")) + "@" + nodename, color.BOLD)
        try:
            avail = mon_data["nodes"][nodename]["services"]["status"][path].get("avail", "n/a")
        except KeyError:
            avail = "undef"
    except ValueError:
        nodename = None
        path = resolve_path(path, namespace)
        node_parent.add_column(strip_path(path, os.environ.get("OSVC_NAMESPACE")), color.BOLD)
        try:
            avail = mon_data["services"][path].get("avail", "n/a")
        except KeyError:
            avail = "undef"
    node_parent.add_column()
    node_parent.add_column(avail, STATUS_COLOR[avail])

def add_children(node, idata, mon_data, namespace):
    children = idata.get("children", [])
    if not children:
        return
    node_children = node.add_node()
    node_children.add_column("children")
    for child in children:
        add_child(child, node_children, mon_data, namespace)

def add_slaves(node, idata, mon_data, namespace):
    slaves = idata.get("slaves", [])
    if not slaves:
        return
    node_slaves = node.add_node()
    node_slaves.add_column("slaves")
    for child in slaves:
        add_child(child, node_slaves, mon_data, namespace)

def add_scaler_slaves(node, idata, mon_data, namespace):
    slaves = idata.get("scaler_slaves", [])
    if not slaves:
        return
    node_slaves = node.add_node()
    node_slaves.add_column("scaler")
    for child in slaves:
        add_child(child, node_slaves, mon_data, namespace)

def add_child(path, node, mon_data, namespace):
    node_child = node.add_node()
    node_child.add_column(strip_path(path, os.environ.get("OSVC_NAMESPACE")), color.BOLD)
    node_child.add_column()
    path = resolve_path(path, namespace)
    try:
        avail = mon_data["services"][path].get("avail", "n/a")
    except KeyError:
        avail = "undef"
    node_child.add_column(avail, STATUS_COLOR[avail])

def get_ers(idata, discard_disabled=False):
    # encap resources
    ers = {}
    for rid in idata.get("resources", {}):
        if not rid.startswith("container"):
            continue
        try:
            ejs = idata["encap"][rid]
            ers[rid] = dispatch_resources(ejs, discard_disabled=discard_disabled)
        except (KeyError, TypeError):
            continue
        except Exception as exc:
            print(exc)
            ers[rid] = {}
    return ers

def add_node_node(node_instances, nodename, idata, mon_data, discard_disabled=False):
    subsets = dispatch_resources(idata, discard_disabled=discard_disabled)

    try:
        node_frozen = mon_data["nodes"][nodename].get("frozen")
    except (KeyError, AttributeError):
        node_frozen = False
    notice = instance_notice(
        overall=idata.get("overall", "n/a"),
        frozen=idata.get("frozen"),
        node_frozen=node_frozen,
        constraints=idata.get("constraints", True),
        provisioned=idata.get("provisioned"),
        monitor=idata.get("monitor"),
        priority=idata.get("priority", Env.default_priority),
    )

    node_nodename = node_instances.add_node()
    node_nodename.add_column(nodename, color.BOLD)
    node_nodename.add_column()
    node_nodename.add_column(idata.get("avail", "n/a"), STATUS_COLOR[idata.get("avail", "n/a")])
    node_nodename.add_column(notice, color.LIGHTBLUE)
    add_subsets(subsets, node_nodename, idata, discard_disabled=discard_disabled)


def service_nodes(path, mon_data):
    nodes = set()
    if not mon_data and is_service(path, local=True):
        return set([Env.nodename])
    for nodename, data in mon_data.get("nodes", {}).items():
        _nodes = set(data.get("services", {}).get("config", {}).get(path, {}).get("scope", []))
        nodes |= _nodes
    return nodes

def format_instance(path, idata, mon_data=None, discard_disabled=False, nodename=None):
    name, namespace, kind = split_path(path)
    svc_notice = get_svc_notice(idata)

    tree = utilities.render.forest.Forest(
        separator=" ",
        widths=(
            (14, None),
            None,
            10,
            None,
        ),
    )
    node_name = tree.add_node()
    node_name.add_column(strip_path(path, os.environ.get("OSVC_NAMESPACE")), color.BOLD)
    node_name.add_column()
    if "cluster" in idata:
        node_name.add_column(idata["cluster"].get("avail", "n/a"), STATUS_COLOR[idata["cluster"].get("avail", "n/a")])
    else:
        node_name.add_column()
    node_name.add_column(svc_notice)
    node_instances = node_name.add_node()
    node_instances.add_column("instances")
    add_instances(node_instances, path, nodename, mon_data)
    if nodename in service_nodes(path, mon_data):
        add_node_node(node_instances, nodename, idata, mon_data, discard_disabled=discard_disabled)
    add_parents(node_name, idata, mon_data, namespace)
    add_children(node_name, idata, mon_data, namespace)
    add_scaler_slaves(node_name, idata, mon_data, namespace)
    add_slaves(node_name, idata, mon_data, namespace)

    tree.out()

