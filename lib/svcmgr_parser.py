"""
svcmgr command line actions and options
"""
from rcGlobalEnv import Storage
from rcOptParser import OptParser
from optparse import Option

PROG = "svcmgr"

OPT = Storage({
    "account": Option(
        "--account", default=False,
        action="store_true", dest="account",
        help="If set the unavailabity period is accounted in the service "
             "availability ratio maintained by the collector."),
    "add": Option(
        "--add", default=None,
        action="store",
        help="A list member to add to the value pointed by :opt:``--param``. "
             "If :opt:``--index`` is set, insert the new element at the "
             "specified position in the list."),
    "attach": Option(
        "--attach", default=False,
        action="store_true", dest="attach",
        help="Attach the modulesets specified in a compliance run."),
    "author": Option(
        "--author", default=None,
        action="store", dest="author",
        help="The acker name to log when acknowledging action log errors"),
    "backlog": Option(
        "--backlog", default=None,
        action="store", dest="backlog",
        help="A size expression limiting the volume of data fetched "
             "from the log file tail. Default is 10k."),
    "begin": Option(
        "--begin", default=None,
        action="store", dest="begin",
        help="A begin date expressed as ``YYYY-MM-DD hh:mm`` limiting the "
             "timerange the action applies to."),
    "cluster": Option(
        "-c", "--cluster", default=False,
        action="store_true", dest="cluster",
        help="Execute the action on all cluster nodes. Aggregate the json "
             "outputs."),
    "color": Option(
        "--color", default="auto",
        action="store", dest="color",
        help="Colorize output. Possible values are:\n\n"
             "* auto: guess based on tty presence\n"
             "* always|yes: always colorize\n"
             "* never|no: never colorize"),
    "comment": Option(
        "--comment", default=None,
        action="store", dest="comment",
        help="A comment to log when acknowldging action log error entries."),
    "config": Option(
        "--config", default=None,
        action="store", dest="parm_config",
        help="The configuration file to use as template when creating or "
             "installing a service"),
    "cron": Option(
        "--cron", default=False,
        action="store_true", dest="cron",
        help="If set, the action is actually executed only if the scheduling"
             "constraints are satisfied."),
    "debug": Option(
        "--debug", default=False,
        action="store_true", dest="debug",
        help="Increase stream log verbosity up to the debug level."),
    "daemon": Option(
        "--daemon", default=False,
        action="store_true", dest="daemon",
        help="A flag inhibiting the daemonization. Set by the "
             "daemonization routine."),
    "disable_rollback": Option(
        "--disable-rollback", default=False,
        action="store_true", dest="disable_rollback",
        help="If set, don't try to rollback resources activated before a "
             "start action interrupts on error."),
    "discard": Option(
        "--discard", default=False,
        action="store_true", dest="discard",
        help="Discard the stashed, invalid, configuration file."),
    "downto": Option(
        "--downto", default=None,
        action="store", dest="upto",
        help="Stop the service down to the specified rid or driver group."),
    "dry_run": Option(
        "--dry-run", default=False,
        action="store_true", dest="dry_run",
        help="Show the action execution plan."),
    "duration": Option(
        "--duration", default=None,
        action="store", dest="duration",
        help="A duration expression like, ``1h10m``."),
    "end": Option(
        "--end", default=None,
        action="store", dest="end",
        help="A end date expressed as ``YYYY-MM-DD hh:mm`` limiting the "
             "timerange the action applies to."),
    "env": Option(
        "--env", default=[],
        action="append", dest="env",
        help="Export the uppercased variable in the os environment.\n\n"
             "With the create action only, set a env section parameter in "
             "the service configuration file. Multiple ``--env <key>=<val>`` "
             "can be specified. For all other actions."),
    "eval": Option(
        "--eval", default=False,
        action="store_true", dest="eval",
        help="If set with the :cmd:`svcmgr get` action, the printed value of "
             ":opt:`--param` is evaluated, scoped and dereferenced."),
    "follow": Option(
        "--follow", default=False,
        action="store_true", dest="follow",
        help="Follow the logs as they come. Use crtl-c to interrupt."),
    "force": Option(
        "-f", "--force", default=False,
        action="store_true", dest="force",
        help="Force action, ignore sanity checks."),
    "format": Option(
        "--format", default=None,
        action="store", dest="format",
        help="Specify a data formatter. Possible values are json, csv"
             " or table."),
    "help": Option(
        "-h", "--help", default=None,
        action="store_true", dest="parm_help",
        help="Show this help message and exit."),
    "hide_disabled": Option(
        "--hide-disabled", default=None,
        action="store_false", dest="show_disabled",
        help="Do not include the disabled resources. This option supersedes "
             "the :kw:`show_disabled` value in the service configuration."),
    "id": Option(
        "--id", default=0,
        action="store", dest="id", type="int",
        help="Specify an object id to act on"),
    "index": Option(
        "--index", default=None,
        action="store", type="int",
        help="The position in the list pointed by --param where to add "
             "the new list element on a set action"),
    "interactive": Option(
        "-i", "--interactive", default=False,
        action="store_true", dest="interactive",
        help="Prompt the user for a choice instead of using defaults, "
             "or failing if no default is defined."),
    "kw": Option(
        "--kw", action="append", dest="kw",
        help="An expression like ``[<section>.]<keyword>[@<scope>][[<index>]]<op><value>`` where\n\n"
             "* <section> can be:\n\n"
             "  * a resource id\n"
             "  * a resource driver group name (fs, ip, ...). In this case, the set applies to all matching resources.\n"
             "* <op> can be:\n\n"
             "  * ``=``\n"
             "  * ``+=``\n"
             "  * ``-=``\n\n"
             "Multiple --kw can be set to apply multiple configuration change "
             "in a file with a single write.\n\n"
             "Examples:\n\n"
             "* app.start=false\n"
             "  Turn off app start for all app resources\n"
             "* app#1.start=true\n"
             "  Turn on app start for app#1\n"
             "* nodes+=node3\n"
             "  Append node3 to nodes\n"
             "* nodes[0]+=node3\n"
             "  Preprend node3 to nodes\n"),
    "like": Option(
        "--like", default="%",
        action="store", dest="like",
        help="A data filtering expression. ``%`` is the multi-character "
             "wildcard. ``_`` is the single-character wildcard. Leading and "
             "trailing ``%`` are automatically set."),
    "local": Option(
        "--local", default=False,
        action="store_true", dest="local",
        help="Execute the service action on the local service "
             "instances only, ignoring cluster-wide considerations."),
    "master": Option(
        "--master", default=False,
        action="store_true", dest="master",
        help="Limit the action scope to the master service resources."),
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
    "node": Option(
        "--node", default="",
        action="store", dest="node",
        help="The node to send a request to. If not specified the local node is targeted."),
    "nopager": Option(
        "--no-pager", default=False,
        action="store_true", dest="nopager",
        help="Do not display the command result in a pager."),
    "parallel": Option(
        "-p", "--parallel", default=False,
        action="store_true", dest="parallel",
        help="Start actions on specified services in parallel. :kw:`max_parallel` "
             "in node.conf limits the number of parallel running subprocesses."),
    "param": Option(
        "--param", default=None,
        action="store", dest="param",
        help="An expression like ``[<section>.]<keyword>`` where\n\n"
             "* <section> can be:\n\n"
             "  * a resource id\n"
             "  * a resource driver group name (fs, ip, ...). In this case, the set applies to all matching resources."),
    "provision": Option(
        "--provision", default=False,
        action="store_true", dest="provision",
        help="Provision the service resources after config file creation. "
             "Defaults to False."),
    "recover": Option(
        "--recover", default=False,
        action="store_true", dest="recover",
        help="Recover the stashed erroneous configuration file "
             "in a :cmd:`svcmgr edit config` command"),
    "refresh": Option(
        "--refresh", default=False,
        action="store_true", dest="refresh",
        help="Drop status caches and re-evaluate before printing."),
    "remove": Option(
        "--remove", default=None,
        action="store",
        help="A list member to drop from the value pointed by :kw:`--param`."),
    "resource": Option(
        "--resource", default=[],
        action="append",
        help="A resource definition in json dictionary format fed to create "
             "or update. The ``type`` key point the driver group name, and "
             "the ``rtype`` key the driver name (translated to type in the "
             "configuration file section)."),
    "rid": Option(
        "--rid", default=None,
        action="store", dest="parm_rid",
        help="A resource specifier expression like ``<spec>[,<spec>]``, where ``<spec>`` can be:\n\n"
             "* A resource id\n"
             "* A driver group name (app, fs, disk, ...)\n\n"
             "Examples:\n\n"
             "* ``app``\n"
             "  all app resources\n"
             "* ``container#1,ip#1``\n"
             "  only container#1 and ip#1\n"
        ),
    "ruleset": Option(
        "--ruleset", default="",
        action="store", dest="ruleset",
        help="Specify the rulesets to limit the action to. The special value ``all`` "
             "can be used in conjonction with detach."),
    "ruleset_date": Option(
        "--ruleset-date", default="",
        action="store", dest="ruleset_date",
        help="Use an historical ruleset, specified by its date."),
    "service": Option(
        "-s", "--service", default=None,
        action="store", dest="parm_svcs",
        help="A service selector expression ``[!]<expr>[<sep>[!]<expr>]`` where:\n\n"
             "- ``!`` is the expression negation operator\n\n"
             "- ``<sep>`` can be:\n\n"
             "  - ``,`` OR expressions\n\n"
             "  - ``+`` AND expressions\n\n"
             "- ``<expr>`` can be:\n\n"
             "  - a shell glob on service names\n\n"
             "  - ``<param><op><value>`` where:\n\n"
             "    - ``<param>`` can be:\n\n"
             "      - ``<rid>:``\n\n"
             "      - ``<group>:``\n\n"
             "      - ``<rid>.<key>``\n\n"
             "      - ``<group>.<key>``\n\n"
             "    - ``<op>`` can be:\n\n"
             "      - ``<``  ``>``  ``<=``  ``>=``  ``=``\n\n"
             "      - ``~`` with regexp value\n\n"
             "Examples:\n\n"
             "- ``*dns,ha*+app.timeout>1``\n\n"
             "- ``ip:+task:``\n\n"
             "- ``!*excluded``\n\n"
             "Note:\n\n"
             "- ``!`` usage requires single quoting the expression to prevent "
             "shell history expansion"),
    "show_disabled": Option(
        "--show-disabled", default=None,
        action="store_true", dest="show_disabled",
        help="Include the disabled resources. This option supersedes "
             "the :kw:`show_disabled` value in the service configuration."),
    "slave": Option(
        "--slave", default=None, action="store", dest="slave",
        help="Limit the action to the service resources in the specified, comma-"
             "separated, slaves."),
    "slaves": Option(
        "--slaves", default=False,
        action="store_true", dest="slaves",
        help="Limit the action scope to service resources in all slaves."),
    "status": Option(
        "--status", default=None,
        action="store", dest="parm_status",
        help="Operate only on service with a local instance in the specified availability status "
             "(up, down, warn, ...)."),
    "subsets": Option(
        "--subsets", default=None,
        action="store", dest="parm_subsets",
        help="Limit the action to the resources in the specified, comma-separated, list of subsets."),
    "tag": Option(
        "--tag", default=None,
        action="store", dest="tag",
        help="The tag name, as shown by :cmd:`svcmgr collector list tags`."),
    "tags": Option(
        "--tags", default=None,
        action="store", dest="parm_tags",
        help="A comma-separated list of resource tags to limit "
             "action to. The ``+`` separator can be used to impose "
             "multiple tag conditions. For example, ``tag1+tag2,tag3`` "
             "limits the action to resources with both tag1 and"
             " tag2, or tag3."),
    "template": Option(
        "--template", default=None,
        action="store", dest="parm_template",
        help="The configuration file template name or id, "
             "served by the collector, to use when creating or "
             "installing a service."),
    "time": Option(
        "--time", default="300",
        action="store", dest="time",
        help="A duration expression like ``1m5s``. The maximum wait time for an "
             "async action to finish. Default is 300 seconds."),
    "to": Option(
        "--to", default=None,
        action="store", dest="parm_destination_node",
        help="The remote node to start or migrate the service to."),
    "unprovision": Option(
        "--unprovision", default=False,
        action="store_true", dest="unprovision",
        help="Unprovision the service resources before config files file deletion. "
             "Defaults to False."),
    "upto": Option(
        "--upto", default=None,
        action="store", dest="upto",
        help="Start the service up to the specified rid or driver group."),
    "value": Option(
        "--value", default=None,
        action="store", dest="value",
        help="The value to set for the keyword pointed by :opt:`--param`"),
    "verbose": Option(
        "--verbose", default=False,
        action="store_true", dest="verbose",
        help="Include more information to some print commands output. "
             "For example, add the ``next run`` column in the output of "
             ":cmd:`svcmgr print schedule`."),
    "wait": Option(
        "--wait", default=False,
        action="store_true", dest="wait",
        help="Wait for asynchronous action termination."),
    "waitlock": Option(
        "--waitlock", default="-1",
        action="store", dest="parm_waitlock",
        help="A duration expression like ``5s``. The maximum wait time when acquiring "
             "the service action lock."),
})

SVCMGR_OPTS = [
    OPT.service,
    OPT.status,
]

GLOBAL_OPTS = SVCMGR_OPTS + [
    OPT.cluster,
    OPT.color,
    OPT.daemon,
    OPT.debug,
    OPT.env,
    OPT.parallel,
    OPT.waitlock,
    OPT.help,
]

ACTION_OPTS = [
    OPT.dry_run,
    OPT.force,
    OPT.local,
    OPT.master,
    OPT.node,
    OPT.rid,
    OPT.slave,
    OPT.slaves,
    OPT.subsets,
    OPT.tags,
]

ASYNC_ACTION_OPTS = [
    OPT.time,
    OPT.wait,
]

START_ACTION_OPTS = [
    OPT.disable_rollback,
]

DAEMON_OPTS = [
    OPT.node,
    OPT.local,
]

ACTIONS = {
    "Service actions": {
        "clear": {
            "msg": "Clear the monitor status of the service on the node pointed "
                   "by --node. If --node is not specified, all nodes are "
                   "cleared. This command can be used to reactivate service "
                   "orchestration blocked by a failed status like ``start failed``.",
            "options": DAEMON_OPTS,
        },
        "dns_update": {
            "msg": "Update the collector dns records for the service. The "
                   "managed dns record is <svcname>.<app>.<collector "
                   "domain>``.",
            "options": ACTION_OPTS,
        },
        "shutdown": {
            "msg": "Stop a service, including its standby resources. The log "
                   "shipping to the collector is synchronous.",
            "options": ACTION_OPTS + ASYNC_ACTION_OPTS,
        },
        "start": {
            "msg": "Start a service. The started instances depend on the "
                   "service placement policy, so the local instance may not "
                   "start. A failover service is considered started when one "
                   "instance is started. A flex service is considered started "
                   "when ``<flex_min_nodes>`` instances are started.",
            "options": ACTION_OPTS + START_ACTION_OPTS + ASYNC_ACTION_OPTS + [
                OPT.upto,
            ],
        },
        "startstandby": {
            "msg": "Start local service instance resources flagged standby.",
            "options": ACTION_OPTS + START_ACTION_OPTS,
        },
        "stop": {
            "msg": "Stop all service instances. The standby resources "
                   "are not stopped, unless :opt:`--force` is specified.",
            "options": ACTION_OPTS + ASYNC_ACTION_OPTS + [
                OPT.downto,
            ],
        },
        "provision": {
            "msg": "Provision the service. Leave the service in frozen, stdby up state.",
            "options": ASYNC_ACTION_OPTS + ACTION_OPTS + START_ACTION_OPTS,
        },
        "unprovision": {
            "msg": "Shutdown and unprovision all service instances. Beware, data will be "
                   "lost upon fs and disk unprovisioning.",
            "options": ASYNC_ACTION_OPTS + ACTION_OPTS,
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
        "ls": {
            "msg": "List the service names with a local instance. Most useful to test "
                   "a service selector expression before running an action.",
        },
        "status": {
            "msg": "Return the local service instance overall status code",
            "options": [
                OPT.refresh,
            ],
        },
        "print_status": {
            "msg": "Display the service status, with a detailed view of the local "
                   "instance.",
            "options": [
                OPT.format,
                OPT.hide_disabled,
                OPT.refresh,
                OPT.show_disabled,
            ],
        },
        "print_resource_status": {
            "msg": "Display a specific service resource status, pointed by"
                   " --rid",
            "options": [
                OPT.format,
                OPT.refresh,
                OPT.rid,
            ],
        },
        "print_config_mtime": {
            "msg": "Display the service local configuration file modification time",
        },
        "freeze": {
            "msg": "Block orchestration on the service.",
            "options": ASYNC_ACTION_OPTS + [
                OPT.node,
                OPT.local,
            ],
        },
        "thaw": {
            "msg": "Unblock orchestration on the service.",
            "options": ASYNC_ACTION_OPTS + [
                OPT.node,
                OPT.local,
            ],
        },
        "toc": {
            "msg": "Trigger the service instance pre_monitor_action script and monitor_action method. Beware, this might crash or reboot the local node.",
            "options": ACTION_OPTS,
        },
        "frozen": {
            "msg": "Report on the current blocking of orchestration on the service.",
        },
        "run": {
            "msg": "Run all tasks, or tasks specified by --rid --tags and "
                   "--subset, disregarding their schedule.",
            "options": ACTION_OPTS + [
                OPT.cron,
            ],
        },
        "presync": {
            "msg": "Execute the presync method of the resource driver for each local service instance resource. These methods usually update var files needing replication on other nodes.",
            "options": ACTION_OPTS,
        },
        "postsync": {
            "msg": "Execute the postsync method of the resource driver for each local service instance resource. These methods usually take appropriate action based on var files received from the primary node.",
            "options": ACTION_OPTS,
        },
        "prstatus": {
            "msg": "Report the status of scsi3 persistent reservations on scsi disks held by the local "
                   "service instance.",
        },
        "restart": {
            "msg": "Chain a local service instance stop and start",
            "options": ACTION_OPTS + START_ACTION_OPTS,
        },
        "resync": {
            "msg": "Chain a local service instance  stop, sync_resync and start",
            "options": ACTION_OPTS + START_ACTION_OPTS,
        },
        "sync_nodes": {
            "msg": "Run the synchronization method of each local service instance sync resource, targetting the peer nodes.",
            "options": ACTION_OPTS,
        },
        "sync_drp": {
            "msg": "Run the synchronization method of each local service instance sync resource, targetting the drp nodes.",
            "options": ACTION_OPTS,
        },
        "sync_quiesce": {
            "msg": "Pause replication of sync.netapp and sync.symsrdf resources.",
            "options": ACTION_OPTS,
        },
        "sync_break": {
            "msg": "Break the disk replication of sync.dcsckpt, sync.hp3par, sync.ibmdssnap, sync.netapp, sync.symclone, sync.symsrdf resources.",
            "options": ACTION_OPTS,
        },
        "sync_split": {
            "msg": "Split the disk replication of sync.symsrdf resources.",
            "options": ACTION_OPTS,
        },
        "sync_establish": {
            "msg": "Establish disk replication of sync.symsrdf resources.",
            "options": ACTION_OPTS,
        },
        "sync_resync": {
            "msg": "Like :cmd:`sync update`, but not triggered by the scheduler "
                   "(thus adapted for clone/snap operations).",
            "options": ACTION_OPTS,
        },
        "sync_full": {
            "msg": "Trigger a full copy of the volume to its target.",
            "options": ACTION_OPTS,
        },
        "sync_restore": {
            "msg": "Trigger a restore of the sync resources data to their "
                   "target path (DANGEROUS: make sure you understand before "
                   "running this action).",
            "options": ACTION_OPTS,
        },
        "sync_update": {
            "msg": "Trigger a one-time resync of the volume to its target.",
            "options": ACTION_OPTS,
        },
        "sync_resume": {
            "msg": "Re-establish a broken storage hardware-assisted "
                   "synchronization.",
            "options": ACTION_OPTS,
        },
        "sync_revert": {
            "msg": "Revert to the pre-failover data (looses current data).",
            "options": ACTION_OPTS,
        },
        "sync_verify": {
            "msg": "Trigger a one-time checksum-based verify of the volume "
                   "and its target.",
            "options": ACTION_OPTS,
        },
        "sync_all": {
            "msg": "Chain sync nodes, sync drp and sync update.",
            "options": ACTION_OPTS + [
                OPT.cron,
            ],
        },
        "push_config": {
            "msg": "Push service configuration to the collector.",
            "options": [
                OPT.cron,
            ],
        },
        "pull": {
            "msg": "Pull a service configuration from the collector, overwritting the currently installed one.",
            "options": [
                OPT.provision,
            ],
        },
        "push_resinfo": {
            "msg": "Push the local service instance resources and application launchers info "
                   "key/value pairs the collector.",
            "options": [
                OPT.cron,
            ],
        },
        "push_service_status": {
            "msg": "Push the local service instance and its resources status to the collector.",
            "options": [
                OPT.cron,
            ],
        },
        "print_base_devs": {
            "msg": "Print the list of base devices the local service instance or the "
                   "specified resources are layered on.",
            "options": [
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
                OPT.format,
                OPT.rid,
                OPT.tags,
                OPT.subsets,
            ],
        },
        "print_devs": {
            "msg": "Aggregate the information of :cmd:`print base devs`, :cmd:`print sub devs` and :cmd:`print exposed devs`.",
            "options": [
                OPT.format,
                OPT.rid,
                OPT.tags,
                OPT.subsets,
            ],
        },
        "switch": {
            "msg": "Stop the running failover service instance and start the "
                   "instance on the peer node specified by :opt:`--to "
                   "<nodename>`.",
            "options": ACTION_OPTS + START_ACTION_OPTS + ASYNC_ACTION_OPTS + [
                OPT.to,
            ],
        },
        "takeover": {
            "msg": "Stop the service on its current node and start on the "
                   "local node.",
            "options": ACTION_OPTS + START_ACTION_OPTS + ASYNC_ACTION_OPTS
        },
        "giveback": {
            "msg": "Stop the service on its current node and start on the "
                   "node chosen by the placement policy.",
            "options": ACTION_OPTS + START_ACTION_OPTS + ASYNC_ACTION_OPTS
        },
        "migrate": {
            "msg": "Live migrate the service to the remote node. "
                   "--to <node> specify the remote node to migrate the "
                   "service to.",
            "options": ACTION_OPTS + START_ACTION_OPTS + ASYNC_ACTION_OPTS + [
                OPT.to,
            ],
        },
        "resource_monitor": {
            "msg": "Refresh the monitored resource status. This action is "
                   "scheduleable, usually every minute.",
            "options": ACTION_OPTS + [
                OPT.cron,
            ],
        },
        "docker": {
            "msg": "Wrap the docker client command, setting automatically "
                   "the socket parameter to join the service-private docker "
                   "daemon. The {as_service}, {images} and {instances} words "
                   "in the wrapped command are replaced by, respectively, "
                   "the registry login username/password/email parameters to "
                   "log as a service using <svcname>@<nodename> as the "
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
        "print_schedule": {
            "msg": "Print the service tasks schedule.",
            "options": [
                OPT.format,
                OPT.verbose,
            ],
        },
        "scheduler": {
            "msg": "Run the service task scheduler.",
        },
        "pg_freeze": {
            "msg": "Freeze the tasks of a process group.",
            "options": ACTION_OPTS,
        },
        "pg_thaw": {
            "msg": "Thaw the tasks of a process group.",
            "options": ACTION_OPTS,
        },
        "pg_kill": {
            "msg": "Kill the tasks of a process group.",
            "options": ACTION_OPTS,
        },
        "logs": {
            "msg": "Display the service logs. All service instances logs are aggregated.",
            "options": [
                OPT.backlog,
                OPT.follow,
                OPT.local,
                OPT.node,
                OPT.nopager,
            ]
        },
    },
    "Service configuration": {
        "print_config": {
            "msg": "Display the service current configuration.",
            "options": [
                OPT.format,
            ],
        },
        "edit_config": {
            "msg": "Edit the service configuration. The new configuration file is actually installed only if it passes validation, so this action is recommended over direct edition.",
            "options": [
                OPT.discard,
                OPT.recover,
            ],
        },
        "validate_config": {
            "msg": "Check the section names and keywords are valid.",
        },
        "create": {
            "msg": "Create a new service.",
            "options": ACTION_OPTS + [
                OPT.config,
                OPT.interactive,
                OPT.provision,
                OPT.resource,
                OPT.template,
            ],
        },
        "update": {
            "msg": "Update definitions in an existing service configuration "
                   "file.",
            "options": ACTION_OPTS + [
                OPT.interactive,
                OPT.provision,
                OPT.resource,
            ],
        },
        "delete": {
            "msg": "Delete a service, or only the resources specified by :opt:`--rid` on the local service instance.",
            "options": ASYNC_ACTION_OPTS + ACTION_OPTS + [
                OPT.unprovision,
            ],
        },
        "set": {
            "msg": "Set a service configuration parameter",
            "options": ACTION_OPTS + [
                OPT.kw,
                OPT.add,
                OPT.index,
                OPT.param,
                OPT.remove,
                OPT.value,
            ],
        },
        "get": {
            "msg": "Get the raw or evaluated value of a service "
                   "configuration keyword.",
            "options": ACTION_OPTS + [
                OPT.eval,
                OPT.param,
            ],
        },
        "unset": {
            "msg": "Unset a node configuration keyword.",
            "options": ACTION_OPTS + [
                OPT.param,
            ],
        },
    },
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
                OPT.format,
            ],
        },
        "collector_checks": {
            "msg": "Display service checks.",
            "options": [
                OPT.format,
            ],
        },
        "collector_disks": {
            "msg": "Display service disks.",
            "options": [
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
                OPT.format,
            ],
        },
        "collector_asset": {
            "msg": "Display asset information known to the collector.",
            "options": [
                OPT.format,
            ],
        },
        "collector_networks": {
            "msg": "Display network information known to the collector for "
                   "each service ip.",
            "options": [
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
                OPT.format,
            ],
        },
        "collector_list_tags": {
            "msg": "List all available tags. Use :opt:`--like` to filter the output.",
            "options": [
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
}

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
    "syncverify",
]

ACTIONS_TRANSLATIONS = {
    "push_env_mtime": "push_config_mtime",
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
    "syncupdate": "sync_update",
    "syncverify": "sync_verify",
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

class SvcmgrOptParser(OptParser):
    """
    The svcmgr-specific options parser class
    """
    def __init__(self, args=None, colorize=True, width=None, formatter=None,
                 indent=6):
        OptParser.__init__(self, args=args, prog=PROG, options=OPT,
                           actions=ACTIONS,
                           deprecated_actions=DEPRECATED_ACTIONS,
                           actions_translations=ACTIONS_TRANSLATIONS,
                           global_options=GLOBAL_OPTS,
                           svcmgr_options=SVCMGR_OPTS,
                           colorize=colorize, width=width,
                           formatter=formatter, indent=indent)

