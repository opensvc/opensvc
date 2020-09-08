"""
The service management command actions and options
"""
import commands.mgr.parser as mp
from core.objects.svc import ACTION_ASYNC
from utilities.optparser import OptParser, Option
from utilities.storage import Storage

PROG = "om svc"

OPT = Storage()
OPT.update(mp.OPT)
OPT.update({
    "account": Option(
        "--account", default=False,
        action="store_true", dest="account",
        help="If set the unavailabity period is accounted in the service "
             "availability ratio maintained by the collector."),
    "attach": Option(
        "--attach", default=False,
        action="store_true", dest="attach",
        help="Attach the modulesets specified in a compliance run."),
    "author": Option(
        "--author", default=None,
        action="store", dest="author",
        help="The acker name to log when acknowledging action log errors"),
    "begin": Option(
        "--begin", default=None,
        action="store", dest="begin",
        help="A begin date expressed as ``YYYY-MM-DD hh:mm`` limiting the "
             "timerange the action applies to."),
    "comment": Option(
        "--comment", default=None,
        action="store", dest="comment",
        help="A comment to log when acknowldging action log error entries."),
    "confirm": Option(
        "--confirm", default=False,
        action="store_true", dest="confirm",
        help="Confirm a run action configured to ask for confirmation. "
             "This can be used when scripting the run or triggering it "
             "from the api."),
    "downto": Option(
        "--downto", default=None,
        action="store", dest="upto",
        help="Stop the service down to the specified rid or driver group."),
    "duration": Option(
        "--duration", default=None,
        action="store", dest="duration",
        help="A duration expression like, ``1h10m``."),
    "end": Option(
        "--end", default=None,
        action="store", dest="end",
        help="A end date expressed as ``YYYY-MM-DD hh:mm`` limiting the "
             "timerange the action applies to."),
    "id": Option(
        "--id", default=0,
        action="store", dest="id", type="int",
        help="Specify an object id to act on"),
    "like": Option(
        "--like", default="%",
        action="store", dest="like",
        help="A data filtering expression. ``%`` is the multi-character "
             "wildcard. ``_`` is the single-character wildcard. Leading and "
             "trailing ``%`` are automatically set."),
    "message": Option(
        "--message", default="",
        action="store", dest="message",
        help="The message to send to the collector for logging."),
    "module": Option(
        "--module", default="",
        action="store", dest="module",
        help="Specify the modules to limit the run to. The modules must be in already attached modulesets."),
    "moduleset": Option(
        "--moduleset", default="",
        action="store", dest="moduleset",
        help="Specify the modulesets to limit the action to. The special value ``all`` "
             "can be used in conjonction with detach."),
    "ruleset": Option(
        "--ruleset", default="",
        action="store", dest="ruleset",
        help="Specify the rulesets to limit the action to. The special value ``all`` "
             "can be used in conjonction with detach."),
    "ruleset_date": Option(
        "--ruleset-date", default="",
        action="store", dest="ruleset_date",
        help="Use an historical ruleset, specified by its date."),
    "sync": Option(
        "--sync", default=False,
        action="store_true", dest="syncrpc",
        help="Use synchronous collector communication. For example, "
             ":cmd:`push resinfo --sync` before a compliance run makes sure "
             "the pushed data has hit the collector database before the "
             "rulesets are contextualized."),
    "tag": Option(
        "--tag", default=None,
        action="store", dest="tag",
        help="The tag name, as shown by :cmd:`collector list tags`."),
    "to": Option(
        "--to", default=None,
        action="store", dest="to",
        help="The remote node to start or migrate the service to. Or the "
             "target number of instance to scale to."),
    "upto": Option(
        "--upto", default=None,
        action="store", dest="upto",
        help="Start the service up to the specified rid or driver group."),
    "verbose": Option(
        "--verbose", default=False,
        action="store_true", dest="verbose",
        help="Include more information to some print commands output. "
             "For example, add the ``next run`` column in the output of "
             ":cmd:`print schedule`."),
})

START_ACTION_OPTS = [
    OPT.disable_rollback,
]

ACTIONS = Storage()
ACTIONS.update(mp.ACTIONS)
ACTIONS["Service and volume object actions"] = {
    "abort": {
        "msg": "Abort the action asynchronously done by the cluster daemons.",
        "options": mp.ASYNC_ACTION_OPTS,
    },
    "clear": {
        "msg": "Clear the monitor status of the service on the node pointed "
               "by --node. If --node is not specified, all nodes are "
               "cleared. This command can be used to reactivate service "
               "orchestration blocked by a failed status like ``start failed``.",
        "options": [
		    OPT.slave,
		    OPT.slaves,
        ],
    },
    "dns_update": {
        "msg": "Update the collector dns records for the service. The "
               "managed dns record is <name>.<app>.<collector "
               "domain>``.",
        "options": mp.ACTION_OPTS,
    },
    "enter": {
        "msg": "Enter the container specified by --rid <rid>, executing "
               "a shell.",
        "options": [
            OPT.rid,
        ],
    },
    "boot": {
        "msg": "Clean up actions executed before the daemon starts. For "
               "example scsi reservation release and vg tags removal. "
               "Never execute this action manually.",
        "options": mp.ACTION_OPTS,
    },
    "shutdown": {
        "msg": "Stop a service, including its standby resources. The log "
               "shipping to the collector is synchronous.",
        "options": mp.ACTION_OPTS + mp.ASYNC_ACTION_OPTS,
    },
    "start": {
        "msg": "Start a service. The started instances depend on the "
               "service placement policy, so the local instance may not "
               "start. A failover service is considered started when one "
               "instance is started. A flex service is considered started "
               "when the number of started instances is between "
               "``<flex_min>`` and ``<flex_max>``.",
        "options": mp.ACTION_OPTS + START_ACTION_OPTS + mp.ASYNC_ACTION_OPTS + [
            OPT.upto,
        ],
    },
    "startstandby": {
        "msg": "Start local service instance resources flagged standby.",
        "options": mp.ACTION_OPTS + START_ACTION_OPTS,
    },
    "stop": {
        "msg": "Stop all service instances. The standby resources "
               "are not stopped, unless :opt:`--force` is specified.",
        "options": mp.ACTION_OPTS + mp.ASYNC_ACTION_OPTS + [
            OPT.downto,
        ],
    },
    "provision": {
        "msg": "Provision the service. Leave the service in frozen, stdby up state.",
        "options": mp.ASYNC_ACTION_OPTS + mp.ACTION_OPTS + START_ACTION_OPTS + [
            OPT.leader,
        ],
    },
    "unprovision": {
        "msg": "Shutdown and unprovision all service instances. Beware, data will be "
               "lost upon fs and disk unprovisioning.",
        "options": mp.ASYNC_ACTION_OPTS + mp.ACTION_OPTS + [
            OPT.leader,
        ],
    },
    "disable": {
        "msg": "Disable resources specified by :opt:`--rid` in services specified by "
               ":opt:`--service`. Specifying no resource disables the whole service.",
        "options": [
            OPT.rid,
            OPT.tags,
            OPT.subsets,
        ],
    },
    "enable": {
        "msg": "Enable resources specified by :opt:`--rid` in services specified by "
               ":opt:`--service`. Specifying no resource enables the whole service.",
        "options": [
            OPT.rid,
            OPT.tags,
            OPT.subsets,
        ],
    },
    "install_data": {
        "msg": "Install secrets and configurations in volume resources with "
               "secrets or configurations mapping configured.",
        "options": [
            OPT.rid,
            OPT.tags,
            OPT.subsets,
        ],
    },
    "print_resource_status": {
        "msg": "Display a specific service resource status, pointed by"
               " --rid",
        "options": [
            OPT.filter,
            OPT.format,
            OPT.refresh,
            OPT.rid,
        ],
    },
    "set_provisioned": {
        "msg": "Set the resources as provisioned.",
        "options": mp.ACTION_OPTS,
    },
    "set_unprovisioned": {
        "msg": "Set the resources as unprovisioned.",
        "options": mp.ACTION_OPTS,
    },
    "freeze": {
        "msg": "Block orchestration on the service.",
        "options": mp.ASYNC_ACTION_OPTS + [
            OPT.master,
            OPT.slave,
            OPT.slaves,
        ],
    },
    "thaw": {
        "msg": "Unblock orchestration on the service.",
        "options": mp.ASYNC_ACTION_OPTS + [
            OPT.master,
            OPT.slave,
            OPT.slaves,
        ],
    },
    "toc": {
        "msg": "Trigger the service instance pre_monitor_action script and monitor_action method. Beware, this might crash or reboot the local node.",
        "options": mp.ACTION_OPTS,
    },
    "frozen": {
        "msg": "Report on the current blocking of orchestration on the service.",
    },
    "run": {
        "msg": "Run all tasks, or tasks specified by --rid --tags and "
               "--subset, disregarding their schedule.",
        "options": mp.ACTION_OPTS + [
            OPT.cron,
            OPT.confirm,
        ],
    },
    "presync": {
        "msg": "Execute the presync method of the resource driver for each local service instance resource. These methods usually update var files needing replication on other nodes.",
        "options": mp.ACTION_OPTS,
    },
    "postsync": {
        "msg": "Execute the postsync method of the resource driver for each local service instance resource. These methods usually take appropriate action based on var files received from the primary node.",
        "options": mp.ACTION_OPTS,
    },
    "prstatus": {
        "msg": "Report the status of scsi3 persistent reservations on scsi disks held by the local "
               "service instance.",
    },
    "restart": {
        "msg": "Chain a local service instance stop and start",
        "options": mp.ACTION_OPTS + START_ACTION_OPTS,
    },
    "resync": {
        "msg": "Chain a local service instance  stop, sync_resync and start",
        "options": mp.ACTION_OPTS + START_ACTION_OPTS,
    },
    "snooze": {
        "msg": "Snooze alerts on the node for :opt:`--duration`",
        "options": [
            OPT.duration,
        ],
    },
    "unsnooze": {
        "msg": "Unsnooze alerts on the node",
        "options": [],
    },
    "support": {
        "msg": "Create a tarball archive of config, var and log files, and upload it to the OpenSVC support site.",
    },
    "sync_nodes": {
        "msg": "Run the synchronization method of each local service instance sync resource, targetting the peer nodes.",
        "options": mp.ACTION_OPTS,
    },
    "sync_drp": {
        "msg": "Run the synchronization method of each local service instance sync resource, targetting the drp nodes.",
        "options": mp.ACTION_OPTS,
    },
    "sync_quiesce": {
        "msg": "Pause replication of sync.netapp and sync.symsrdf resources.",
        "options": mp.ACTION_OPTS,
    },
    "sync_break": {
        "msg": "Break the disk replication of sync.hp3par, sync.ibmdssnap, sync.netapp, sync.symclone, sync.symsrdf resources.",
        "options": mp.ACTION_OPTS,
    },
    "sync_split": {
        "msg": "Split the disk replication of sync.symsrdf resources.",
        "options": mp.ACTION_OPTS,
    },
    "sync_establish": {
        "msg": "Establish disk replication of sync.symsrdf resources.",
        "options": mp.ACTION_OPTS,
    },
    "sync_resync": {
        "msg": "Like :cmd:`sync update`, but not triggered by the scheduler "
               "(thus adapted for clone/snap operations).",
        "options": mp.ACTION_OPTS,
    },
    "sync_full": {
        "msg": "Trigger a full copy of the volume to its target.",
        "options": mp.ACTION_OPTS,
    },
    "sync_restore": {
        "msg": "Trigger a restore of the sync resources data to their "
               "target path (DANGEROUS: make sure you understand before "
               "running this action).",
        "options": mp.ACTION_OPTS,
    },
    "sync_update": {
        "msg": "Trigger a one-time resync of the volume to its target.",
        "options": mp.ACTION_OPTS,
    },
    "sync_resume": {
        "msg": "Re-establish a broken storage hardware-assisted "
               "synchronization.",
        "options": mp.ACTION_OPTS,
    },
    "sync_revert": {
        "msg": "Revert to the pre-failover data (looses current data).",
        "options": mp.ACTION_OPTS,
    },
    "sync_verify": {
        "msg": "Trigger a one-time checksum-based verify of the volume "
               "and its target.",
        "options": mp.ACTION_OPTS,
    },
    "sync_all": {
        "msg": "Chain sync nodes, sync drp and sync update.",
        "options": mp.ACTION_OPTS + [
            OPT.cron,
        ],
    },
    "push_config": {
        "msg": "Push service configuration to the collector.",
        "options": [
            OPT.cron,
        ],
    },
    "push_status": {
        "msg": "Push service instance status to the collector synchronously.",
    },
    "push_encap_config": {
        "msg": "Push service configuration to the containers hosting an encapsulated service.",
        "options": [
            OPT.cron,
        ],
    },
    "pull": {
        "msg": "Pull a service configuration from the collector, overwritting the currently installed one.",
        "options": [
            OPT.disable_rollback,
            OPT.provision,
        ],
    },
    "push_resinfo": {
        "msg": "Push the local service instance resources and application launchers info "
               "key/value pairs the collector.",
        "options": [
            OPT.cron,
            OPT.sync,
        ],
    },
    "print_base_devs": {
        "msg": "Print the list of base devices the local service instance or the "
               "specified resources are layered on.",
        "options": [
            OPT.filter,
            OPT.format,
            OPT.rid,
            OPT.tags,
            OPT.subsets,
        ],
    },
    "print_exposed_devs": {
        "msg": "Print the list of devices the local service instance or the specified "
               "resources expose.",
        "options": [
            OPT.filter,
            OPT.format,
            OPT.rid,
            OPT.tags,
            OPT.subsets,
        ],
    },
    "print_sub_devs": {
        "msg": "Print the list of devices the local service instance or the specified "
               "resources are layered on.",
        "options": [
            OPT.filter,
            OPT.format,
            OPT.rid,
            OPT.tags,
            OPT.subsets,
        ],
    },
    "print_devs": {
        "msg": "Aggregate the information of :cmd:`print base devs`, :cmd:`print sub devs` and :cmd:`print exposed devs`.",
        "options": [
            OPT.filter,
            OPT.format,
            OPT.rid,
            OPT.tags,
            OPT.subsets,
        ],
    },
    "scale": {
        "msg": "Create-provision or delete-unprovision instances to meet "
               "service scale target.",
        "options": [
            OPT.to
        ],
    },
    "switch": {
        "msg": "Stop the running failover service instance and start the "
               "instance on the peer node specified by :opt:`--to "
               "<nodename>`.",
        "options": START_ACTION_OPTS + mp.ASYNC_ACTION_OPTS + [
            OPT.to,
        ],
    },
    "takeover": {
        "msg": "Stop the service on its current node and start on the "
               "local node.",
        "options": START_ACTION_OPTS + mp.ASYNC_ACTION_OPTS
    },
    "giveback": {
        "msg": "Stop the service on its current node and start on the "
               "node chosen by the placement policy.",
        "options": START_ACTION_OPTS + mp.ASYNC_ACTION_OPTS
    },
    "migrate": {
        "msg": "Live migrate the service to the remote node. "
               "--to <node> specify the remote node to migrate the "
               "service to.",
        "options": START_ACTION_OPTS + mp.ASYNC_ACTION_OPTS + [
            OPT.to,
        ],
    },
    "move": {
        "msg": "Ask the daemon to orchestrate a stop of the service instances "
               "running on nodes not in the specified target, and start "
               "instances on the specified target nodes. The target is "
               "specified by :opt:`--to <nodename>,<nodename>`.",
        "options": START_ACTION_OPTS + mp.ASYNC_ACTION_OPTS + [
            OPT.to,
        ],
    },
    "resource_monitor": {
        "msg": "Refresh the monitored resource status. This action is "
               "scheduleable, usually every minute.",
        "options": mp.ACTION_OPTS + [
            OPT.cron,
        ],
    },
    "oci": {
        "msg": "Wrap the podman or docke client command, setting automatically "
               "the namespace, cni-config-dir options and eventually "
               "the --root and --runroot options for services configured "
               "for private storage. The {as_service}, {images} and "
               "{instances} words "
               "in the wrapped command are replaced by, respectively, "
               "the registry login username/password/email parameters to "
               "log as a service using <path>@<nodename> as the "
               "username and the node uuid as password (which is what "
               "is expected when the opensvc collector is used as the "
               "JWT manager for the registry), the set of podman "
               "container names and images for container resources "
               "passing the --tags, --rid and --subsets filters. This is "
               "useful to remove all instances of a service or all "
               "instances of resources with a tag like 'frontend'. Note "
               "the opensvc filters must be positioned before the docker "
               "command in the arguments list.",
    },
    "podman": {
        "msg": "Wrap the podman client command, setting automatically "
               "the namespace, cni-config-dir options and eventually "
               "the --root and --runroot options for services configured "
               "for private storage. The {as_service}, {images} and "
               "{instances} words "
               "in the wrapped command are replaced by, respectively, "
               "the registry login username/password/email parameters to "
               "log as a service using <path>@<nodename> as the "
               "username and the node uuid as password (which is what "
               "is expected when the opensvc collector is used as the "
               "JWT manager for the registry), the set of podman "
               "container names and images for container resources "
               "passing the --tags, --rid and --subsets filters. This is "
               "useful to remove all instances of a service or all "
               "instances of resources with a tag like 'frontend'. Note "
               "the opensvc filters must be positioned before the docker "
               "command in the arguments list.",
    },
    "docker": {
        "msg": "Wrap the docker client command, setting automatically "
               "the socket parameter to join the service-private docker "
               "daemon. The {as_service}, {images} and {instances} words "
               "in the wrapped command are replaced by, respectively, "
               "the registry login username/password/email parameters to "
               "log as a service using <path>@<nodename> as the "
               "username and the node uuid as password (which is what "
               "is expected when the opensvc collector is used as the "
               "JWT manager for the registry), the set of docker "
               "instance names and images for container resources "
               "passing the --tags, --rid and --subsets filters. This is "
               "useful to remove all instances of a service or all "
               "instances of resources with a tag like 'frontend'. Note "
               "the opensvc filters must be positioned before the docker "
               "command in the arguments list.",
    },
    "print_resinfo": {
        "msg": "Display the service resource and env section key/val pairs "
               "pushed to the collector.",
        "options": [
            OPT.filter,
            OPT.format,
        ],
    },
    "print_schedule": {
        "msg": "Print the service tasks schedule.",
        "options": [
            OPT.filter,
            OPT.format,
            OPT.verbose,
        ],
    },
}

PG_ACTIONS = {
    "pg_pids": {
        "msg": "Display the tasks of the service process groups or selected resources process groups.",
        "options": [
            OPT.filter,
            OPT.format,
        ],
    },
    "pg_freeze": {
        "msg": "Freeze the tasks of a process group.",
        "options": mp.ACTION_OPTS,
    },
    "pg_thaw": {
        "msg": "Thaw the tasks of a process group.",
        "options": mp.ACTION_OPTS,
    },
    "pg_kill": {
        "msg": "Kill the tasks of a process group.",
        "options": mp.ACTION_OPTS,
    },
    "pg_update": {
        "msg": "Update cappings of process groups to reflect configuration changes.",
        "options": mp.ACTION_OPTS,
    },
    "pg_remove": {
        "msg": "Remove the process group, if empty.",
        "options": mp.ACTION_OPTS,
    },
    "pg_stats": {
        "msg": "Display key statistics of the process group.",
        "options": mp.ACTION_OPTS + [
            OPT.format,
        ],
    },
}
ACTIONS["Service and volume object actions"].update(PG_ACTIONS)

ACTIONS.update({
    "Compliance": {
        "compliance_auto": {
            "msg": "Run compliance checks or fixes, depending on the autofix "
                   "module property values.",
            "options": [
                OPT.attach,
                OPT.cron,
                OPT.force,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        "compliance_check": {
            "msg": "Run compliance checks.",
            "options": [
                OPT.attach,
                OPT.force,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        "compliance_fix": {
            "msg": "Run compliance fixes.",
            "options": [
                OPT.attach,
                OPT.force,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        "compliance_fixable": {
            "msg": "Verify compliance fixes prerequisites.",
            "options": [
                OPT.attach,
                OPT.force,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        "compliance_env": {
            "msg": "Show the environment variables set during a compliance module run.",
            "options": [
                OPT.module,
                OPT.moduleset,
            ],
        },
        "compliance_show_status": {
            "msg": "Show compliance modules status.",
        },
        "compliance_show_moduleset": {
            "msg": "Show compliance rules applying to this service.",
        },
        "compliance_list_moduleset": {
            "msg": "List available compliance modulesets. Setting :opt:`--moduleset f%` "
                   "limits the resultset to modulesets matching the ``f%`` pattern.",
            'options': [
                OPT.moduleset,
            ],
        },
        "compliance_list_ruleset": {
            "msg": "List available compliance rulesets. Setting :opt:`--ruleset f%` limits "
                   "the scope to rulesets matching the ``f%`` pattern.",
            'options': [
                OPT.ruleset,
            ],
        },
        "compliance_show_ruleset": {
            "msg": "Show compliance rules applying to this service.",
        },
        "compliance_attach": {
            "msg": "Attach rulesets specified by :opt:`--ruleset` and modulesets "
                   "specified by :opt:`--moduleset` to this service. Attached modulesets "
                   "are scheduled for check or autofix.",
            "options": [
                OPT.moduleset,
                OPT.ruleset,
            ],
        },
        "compliance_detach": {
            "msg": "Detach rulesets specified by :opt:`--ruleset` and modulesets "
                   "specified by :opt:`--moduleset` from this service. Detached "
                   "modulesets are no longer scheduled for check and autofix.",
            "options": [
                OPT.moduleset,
                OPT.ruleset,
            ],
        },
    },
    "Collector management": {
        "collector_ack_unavailability": {
            "msg": "Acknowledge an unavailability period. The period is "
                   "specified by :opt:`--begin` :opt:`--end` or :opt:`--begin` :opt:`--duration`. "
                   ":opt:`--begin` defaults to now.",
            "options": [
                OPT.author,
                OPT.account,
                OPT.begin,
                OPT.end,
                OPT.comment,
                OPT.duration,
            ],
        },
        "collector_list_unavailability_ack": {
            "msg": "List acknowledged periods for the service.",
            "options": [
                OPT.author,
                OPT.begin,
                OPT.end,
                OPT.comment,
            ],
        },
        "collector_list_actions": {
            "msg": "List actions on the service, Whatever the node, during "
                   "the period specified by --begin/--end. --end defaults to "
                   "now. --begin defaults to 7 days ago.",
            "options": [
                OPT.begin,
                OPT.end,
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_ack_action": {
            "msg": "Acknowledge an action error on the service. An "
                   "acknowlegment can be completed by --author (defaults "
                   "to root@nodename) and --comment.",
            "options": [
                OPT.author,
                OPT.comment,
            ],
        },
        "collector_show_actions": {
            "msg": "Show actions detailed log. A single action is specified "
                   "by --id. A range is specified by --begin/--end dates. "
                   "--end defaults to now. --begin defaults to 7 days ago.",
            "options": [
                OPT.begin,
                OPT.id,
                OPT.end,
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_checks": {
            "msg": "Display service checks.",
            "options": [
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_disks": {
            "msg": "Display service disks.",
            "options": [
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_log": {
            "msg": "Log a message in the collector service log.",
            "options": [
                OPT.message,
            ],
        },
        "collector_alerts": {
            "msg": "Display service alerts.",
            "options": [
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_events": {
            "msg": "Display the service events during the period specified by "
                   "--begin/--end. --end defaults to now. --begin defaults "
                   "to 7 days ago.",
            "options": [
                OPT.begin,
                OPT.end,
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_asset": {
            "msg": "Display asset information known to the collector.",
            "options": [
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_networks": {
            "msg": "Display network information known to the collector for "
                   "each service ip.",
            "options": [
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_tag": {
            "msg": "Set a service tag (pointed by --tag).",
            "options": [
                OPT.tag,
            ],
        },
        "collector_untag": {
            "msg": "Unset a service tag (pointed by --tag).",
            "options": [
                OPT.tag,
            ],
        },
        "collector_show_tags": {
            "msg": "List all service tags.",
            "options": [
                OPT.filter,
                OPT.format,
            ],
        },
        "collector_list_tags": {
            "msg": "List all available tags. Use :opt:`--like` to filter the output.",
            "options": [
                OPT.filter,
                OPT.format,
                OPT.like,
            ],
        },
        "collector_create_tag": {
            "msg": "Create a new tag.",
            "options": [
                OPT.tag,
            ],
        },
    },
})

DEPRECATED_OPTIONS = [
    "daemon",
]

DEPRECATED_ACTIONS = [
    "collector_json_alerts",
    "collector_json_asset",
    "collector_json_checks",
    "collector_json_disks",
    "collector_json_events",
    "collector_json_list_actions",
    "collector_json_list_unavailability_ack",
    "collector_json_networks",
    "collector_json_show_actions",
    "collector_json_status",
    "push_appinfo",
    "json_config",
    "json_devs",
    "json_sub_devs",
    "json_base_devs",
    "json_env",
    "json_schedule",
    "json_status",
    "startapp",
    "startcontainer",
    "startdisk",
    "startfs",
    "startip",
    "startshare",
    "stopapp",
    "stopcontainer",
    "stopdisk",
    "stopfs",
    "stopip",
    "stopshare",
    "syncall",
    "syncbreak",
    "syncestablish",
    "syncnodes",
    "syncdrp",
    "syncfullsync",
    "syncquiesce",
    "syncresync",
    "syncsplit",
    "syncupdate",
    "syncresume",
    "syncrevert",
    "syncswap",
    "syncverify",
]

ACTIONS_TRANSLATIONS = {
    "push_env": "push_config",
    "push": "push_config",
    "json_env": "json_config",
    "startapp": {"action": "start", "mangle": lambda x: add_rid(x, ["app"])},
    "startip": {"action": "start", "mangle": lambda x: add_rid(x, ["ip"])},
    "startfs": {"action": "start", "mangle": lambda x: add_rid(x, ["disk", "fs"])},
    "startdisk": {"action": "start", "mangle": lambda x: add_rid(x, ["disk"])},
    "startshare": {"action": "start", "mangle": lambda x: add_rid(x, ["share"])},
    "startcontainer": {"action": "start", "mangle": lambda x: add_rid(x, ["container"])},
    "stopapp": {"action": "stop", "mangle": lambda x: add_rid(x, ["app"])},
    "stopip": {"action": "stop", "mangle": lambda x: add_rid(x, ["ip"])},
    "stopfs": {"action": "stop", "mangle": lambda x: add_rid(x, ["disk", "fs"])},
    "stopdisk": {"action": "stop", "mangle": lambda x: add_rid(x, ["disk"])},
    "stopshare": {"action": "stop", "mangle": lambda x: add_rid(x, ["share"])},
    "stopcontainer": {"action": "stop", "mangle": lambda x: add_rid(x, ["container"])},
    "prstart": {"action": "start", "mangle": lambda x: add_rid(x, ["disk.scsireserv"])},
    "prstop": {"action": "stop", "mangle": lambda x: add_rid(x, ["disk.scsireserv"])},
    "syncall": "sync_all",
    "syncbreak": "sync_break",
    "syncdrp": "sync_drp",
    "syncestablish": "sync_establish",
    "syncfullsync": "sync_full",
    "syncnodes": "sync_nodes",
    "syncquiesce": "sync_quiesce",
    "syncrestore": "sync_restore",
    "syncresume": "sync_resume",
    "syncresync": "sync_resync",
    "syncrevert": "sync_revert",
    "syncsplit": "sync_split",
    "syncswap": "sync_swap",
    "syncupdate": "sync_update",
    "syncverify": "sync_verify",
    "unfreeze": "thaw",
}

def add_rid(options, new_rids):
    if options.parm_rid is None:
        options.parm_rid = ",".join(new_rids)
        return options
    # discard incompatible rids
    rids = [rid for rid in options.parm_rid.split(",") if \
            rid.split('#')[0] in new_rids]
    if len(rids) == 0:
        options.parm_rid = "impossible"
        return options
    options.parm_rid = ",".join(rids)
    return options

class SvcOptParser(OptParser):
    """
    The service management command options parser class.
    """
    def __init__(self, args=None, colorize=True, width=None, formatter=None,
                 indent=6):
        OptParser.__init__(self, args=args, prog=PROG, options=OPT,
                           actions=ACTIONS,
                           deprecated_options=DEPRECATED_OPTIONS,
                           deprecated_actions=DEPRECATED_ACTIONS,
                           actions_translations=ACTIONS_TRANSLATIONS,
                           global_options=mp.GLOBAL_OPTS,
                           svc_select_options=mp.SVC_SELECT_OPTS,
                           colorize=colorize, width=width,
                           formatter=formatter, indent=indent, async_actions=ACTION_ASYNC)

