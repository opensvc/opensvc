"""
Event messages to log, indexed by (event id, reason).
"""

from __future__ import print_function

EVENTS = {
    ("arbitrator_up", None): "arbitrator {arbitrator} is now reachable",
    ("arbitrator_down", None): "arbitrator {arbitrator} is no longer reachable",
    ("blacklist_add", None): "sender {sender} blacklisted",
    ("crash", "split"): "cluster is split, we don't have quorum: {node_votes}+{arbitrator_votes}/{voting} votes {pro_voters}",
    ("reboot", "split"): "cluster is split, we don't have quorum: {node_votes}+{arbitrator_votes}/{voting} votes {pro_voters}",
    ("forget_peer", "no_rx"): "no rx thread still receive from node {peer} and maintenance grace period expired. flush its data",
    ("hb_beating", None): "node {nodename} hb status stale => beating",
    ("hb_stale", None): "node {nodename} hb status beating => stale",
    ("node_config_change", None): "node config change",
    ("node_freeze", "target"): "freeze node",
    ("node_thaw", None): "thaw node",
    ("node_freeze", "kern_freeze"): "freeze node due to kernel cmdline flag.",
    ("node_freeze", "upgrade"): "freeze node for upgrade until the cluster is complete",
    ("node_freeze", "rejoin_expire"): "freeze node, the cluster is not complete on rejoin grace period expiration",
    ("node_freeze", "merge_frozen"): "freeze node, node {peer} was frozen while we were down",
    ("node_thaw", "upgrade"): "thaw node after upgrade, the cluster is complete",
    ("max_resource_restart", None): "max restart ({restart}) reached for resource {rid} ({resource.label})",
    ("max_stdby_resource_restart", None): "max restart ({restart}) reached for standby resource {rid} ({resource.label})",
    ("monitor_started", None): "monitor started",
    ("resource_toc", None): "toc for resource {rid} ({resource.label}) {resource.status} {resource.log}",
    ("resource_would_toc", "no_candidate"): "would toc for resource {rid} ({resource.label}) {resource.status} {resource.log}, but no node is candidate for takeover.",
    ("resource_degraded", None): "resource {rid} ({resource.label}) degraded to {resource.status} {resource.log}",
    ("resource_restart", None): "restart resource {rid} ({resource.label}) {resource.status} {resource.log}, try {try}/{restart}",
    ("stdby_resource_restart", None): "start standby resource {rid} ({resource.label}) {resource.status} {resource.log}, try {try}/{restart}",
    ("service_config_installed", None): "config fetched from node {from} is now installed",
    ("instance_abort", "target"): "abort {instance.topology} {instance.avail} instance {instance.monitor.local_expect} action to satisfy the {instance.monitor.global_expect} target",
    ("instance_delete", "target"): "delete {instance.topology} {instance.avail} instance to satisfy the {instance.monitor.global_expect} target",
    ("instance_freeze", "target"): "freeze instance to satisfy the {instance.monitor.global_expect} target",
    ("instance_freeze", "install"): "freeze instance on install",
    ("instance_freeze", "merge_frozen"): "freeze instance on rejoin because instance on {peer} is frozen",
    ("instance_provision", "target"): "provision {instance.topology} {instance.avail} instance to satisfy the {instance.monitor.global_expect} target",
    ("instance_purge", "target"): "purge {instance.topology} {instance.avail} instance to satisfy the {instance.monitor.global_expect} target",
    ("instance_start", "single_node"): "start idle single node {instance.avail} instance",
    ("instance_start", "from_ready"): "start {instance.topology} {instance.avail} instance ready for {since} seconds",
    ("instance_start", "target"): "start {instance.topology} {instance.avail} instance to satisfy the {instance.monitor.global_expect} target",
    ("instance_stop", "target"): "stop {instance.topology} {instance.avail} instance to satisfy the {instance.monitor.global_expect} target",
    ("instance_stop", "flex_threshold"): "stop {instance.topology} {instance.avail} instance to meet threshold constraints: {up}/{instance.flex_target}",
    ("instance_thaw", "target"): "thaw instance to satisfy the {instance.monitor.global_expect} target",
    ("instance_unprovision", "target"): "unprovision {instance.topology} {instance.avail} instance to satisfy the {instance.monitor.global_expect} target",
    ("scale_up", None): "misses {delta} instance to reach scale target {instance.scale}",
    ("scale_down", None): "exceeds {delta} instance to reach scale target {instance.scale}",
}


def doc():
    buff = "Daemon Events\n"
    buff += "=============\n\n"
    for (eid, reason), msg in sorted(EVENTS.items(), key=lambda x: x[0][0] + x[0][1] if x[0][1] else ""):
        if reason:
            title = "Id ``%s``, Reason ``%s``" % (eid, reason)
        else:
            title = "Id ``%s``" % eid
        length = len(title)
        buff += "%s\n" % title
        buff += "-" * length + "\n\n"
        buff += "%s\n\n" % msg
    return buff


if __name__ == "__main__":
    print(doc())
