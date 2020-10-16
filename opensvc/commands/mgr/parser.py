from utilities.optparser import Option
from utilities.storage import Storage

OPT = Storage({
    "add": Option(
        "--add", default=None,
        action="store",
        help="A list member to add to the value pointed by :opt:`--param`. "
             "If :opt:`--index` is set, insert the new element at the "
             "specified position in the list."),
    "backlog": Option(
        "--backlog", default=None,
        action="store", dest="backlog",
        help="A size expression limiting the volume of data fetched "
             "from the log file tail. Default is 10k."),
    "color": Option(
        "--color", default="auto",
        action="store", dest="color",
        help="Colorize output. Possible values are:\n\n"
             "* auto: guess based on tty presence\n"
             "* always|yes: always colorize\n"
             "* never|no: never colorize"),
    "config": Option(
        "--config", default=None,
        action="store", dest="parm_config",
        help="The configuration to use as template when creating or "
             "installing a service. The value can be ``-`` or ``/dev/stdin`` "
             "to read the json-formatted configuration from stdin, or a file "
             "path, or uri pointing to a ini-formatted configuration, or a "
             "service selector expression (ATTENTION with cloning existing live "
             "services that include more than containers, volumes and backend "
             "ip addresses ... this could cause disruption on the cloned service)."),
    "cron": Option(
        "--cron", default=False,
        action="store_true", dest="cron",
        help="If set, the action is actually executed only if the scheduling"
             "constraints are satisfied."),
    "debug": Option(
        "--debug", default=False,
        action="store_true", dest="debug",
        help="Increase stream and file log verbosity up to the debug level."),
    "daemon": Option(
        "--daemon", default=False,
        action="store_true", dest="daemon",
        help="A flag inhibiting the command daemonization. Set by the "
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
    "dry_run": Option(
        "--dry-run", default=False,
        action="store_true", dest="dry_run",
        help="Show the action execution plan."),
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
        help="If set with the :cmd:`get` action, the printed value of "
             ":opt:`--param` is evaluated, scoped and dereferenced. If set "
             "with the :cmd:`set` action, the current value is "
             "evaluated before mangling."),
    "filter": Option(
        "--filter", default="",
        action="store", dest="jsonpath_filter",
        help="A JSONPath expression to filter a JSON output."),
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
        help="Specify a data formatter. Possible values are json, flat_json, "
             "csv or table. csv and table formatters are available only for "
             "commands returning tabular data."),
    "help": Option(
        "-h", "--help", default=None,
        action="store_true", dest="parm_help",
        help="Show this help message and exit."),
    "hide_disabled": Option(
        "--hide-disabled", default=None,
        action="store_false", dest="show_disabled",
        help="Do not include the disabled resources. This option supersedes "
             "the :kw:`show_disabled` value in the service configuration."),
    "impersonate": Option(
        "--impersonate", default=None,
        action="store",
        help="Impersonate a peer node when evaluating keywords."),
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
    "interval": Option(
        "--interval", default=0, action="store",
        dest="interval", type="int",
        help="with --watch, set the refresh interval. defaults "
             "to 0, to refresh on event only."),
    "kw": Option(
        "--kw", action="append", dest="kw",
        help="An expression like ``[<section>.]<keyword>[@<scope>][[<index>]]<op><value>`` where\n\n"
             "* <section> can be:\n\n"
             "  * a resource id\n"
             "  * a resource driver group name (fs, ip, ...). For the set and unset actions only, set the keyword for all matching resources.\n"
             "* <op> can be:\n\n"
             "  * ``=``  set as new value\n"
             "  * ``-=`` remove value from the current list\n"
             "  * ``+=`` append value to the current list\n"
             "  * ``|=`` append value to the current list if not already included\n\n"
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
    "leader": Option(
        "--leader", default=None,
        action="store_true", dest="leader",
        help="Switch the provision action behaviour to leader, ie provision shared resources that are not provisionned by default."),
    "local": Option(
        "--local", default=False,
        action="store_true", dest="local",
        help="Execute the service action on the local service "
             "instances only, ignoring cluster-wide considerations."),
    "master": Option(
        "--master", default=False,
        action="store_true", dest="master",
        help="Limit the action scope to the master service resources."),
    "namespace": Option(
        "--namespace",
        action="store", dest="namespace",
        help="The namespace to switch to for the action. Namespaces are cluster partitions. A default namespace can be set for the session setting the OSVC_NAMESPACE environment variable."),
    "node": Option(
        "--node", default="",
        action="store", dest="node",
        help="The node to send a request to. If not specified the local node is targeted."),
    "nolock": Option(
        "--nolock", default=False,
        action="store_true", dest="nolock",
        help="Don't acquire the action lock. Dangerous, but can be useful to set parameters from an action trigger."),
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
             "  * a resource driver group name (fs, ip, ...). For the set and unset actions only, set the keyword for all matching resources."),
    "provision": Option(
        "--provision", default=False,
        action="store_true", dest="provision",
        help="Provision the service resources after config file creation. "
             "Defaults to False."),
    "purge_collector": Option(
        "--purge-collector", default=False,
        action="store_true", dest="purge_collector",
        help="On service delete, also remove the service collector-side"),
    "recover": Option(
        "--recover", default=False,
        action="store_true", dest="recover",
        help="Recover the stashed erroneous configuration file "
             "in a :cmd:`edit config` command"),
    "refresh": Option(
        "-r", "--refresh", default=False,
        action="store_true", dest="refresh",
        help="Drop status caches and re-evaluate before printing."),
    "remove": Option(
        "--remove", default=None,
        action="store",
        help="A list member to drop from the value pointed by :kw:`--param`."),
    "resource": Option(
        "--resource",
        action="append",
        help="A resource definition in json dictionary format fed to create "
             "or update. The ``rtype`` key point the driver group name, and "
             "the ``type`` key the driver name (translated to type in the "
             "configuration file section)."),
    "restore": Option(
        "--restore", default=False,
        action="store_true", dest="restore",
        help="Keep the same service id as the template or config file referenced by the create action. The default behaviour is to generate a new id."),
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
    "sections": Option(
        "--sections",
        action="store", dest="sections",
        help="the comma-separated list of sections to display. "
             "if not set, all sections are displayed. sections "
             "names are: threads,arbitrators,nodes,services."),
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
             "      - ``<single value jsonpath expression on the $.monitor.services.<path> dictionary extended under the 'nodes' key by each instance 'status' and 'config' data>``\n\n"
             "    - ``<op>`` can be:\n\n"
             "      - ``<``  ``>``  ``<=``  ``>=``  ``=``\n\n"
             "      - ``~`` the string or any list element matches the regexp value\n\n"
             "      - ``~=`` the string matches regexp value or any list element is the value\n\n"
             "Examples:\n\n"
             "- ``*dns,ha*+app.timeout>1``\n\n"
             "- ``ip:+task:``\n\n"
             "- ``!*excluded``\n\n"
             "- ``$.avail=warn``\n\n"
             "- ``$.nodes.*.status.avail=warn``\n\n"
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
    "stats": Option(
        "--stats", default=False,
        action="store_true", dest="stats",
        help="Show system resources usage metrics and refresh the information every --interval."),
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
    "unprovision": Option(
        "--unprovision", default=False,
        action="store_true", dest="unprovision",
        help="Unprovision the service resources before config files file deletion. "
             "Defaults to False."),
    "value": Option(
        "--value", default=None,
        action="store", dest="value",
        help="The value to set for the keyword pointed by :opt:`--param`"),
    "wait": Option(
        "--wait", default=False,
        action="store_true", dest="wait",
        help="Wait for asynchronous action termination."),
    "waitlock": Option(
        "--waitlock", default="-1",
        action="store", dest="parm_waitlock",
        help="A duration expression like ``5s``. The maximum wait time when acquiring "
             "the service action lock."),
    "watch": Option(
        "-w", "--watch", default=False,
        action="store_true", dest="watch",
        help="refresh the information every --interval."),
})

SVC_SELECT_OPTS = [
    OPT.namespace,
    OPT.service,
    OPT.status,
    OPT.node,
    OPT.local,
]

GLOBAL_OPTS = SVC_SELECT_OPTS + [
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
    OPT.master,
    OPT.nolock,
    OPT.rid,
    OPT.slave,
    OPT.slaves,
    OPT.subsets,
    OPT.tags,
]

CONFIG_OPTS = [
    OPT.force,
    OPT.master,
    OPT.nolock,
    OPT.rid,
    OPT.slave,
    OPT.slaves,
    OPT.subsets,
    OPT.tags,
]

ASYNC_ACTION_OPTS = [
    OPT.time,
    OPT.wait,
    OPT.stats,
    OPT.watch,
    OPT.interval,
]


ACTIONS = {
    "Common object actions": {
        "deploy": {
            "msg": "Create and provision a new service.",
            "options": CONFIG_OPTS + [
                OPT.config,
                OPT.disable_rollback,
                OPT.interactive,
                OPT.kw,
                OPT.leader,
                OPT.restore,
                OPT.template,
            ],
        },
        "logs": {
            "msg": "Display the service logs. All service instances logs are aggregated.",
            "options": [
                OPT.backlog,
                OPT.follow,
                OPT.nopager,
            ]
        },
        "ls": {
            "msg": "List the service names with a local instance. Most useful to test "
                   "a service selector expression before running an action.",
            "options": [
                OPT.filter,
                OPT.format,
            ],
        },
        "monitor": {
            "msg": "Display or watch the synthetic service status, and perf metrics.",
            "options": [
                OPT.sections,
                OPT.stats,
                OPT.watch,
                OPT.interval,
                OPT.format,
            ],
        },
        "print_status": {
            "msg": "Display the service status, with a detailed view of the local "
                   "instance.\n\n"
                   "Resources Flags:\n\n"
                   "(1) ``R``   Running,           ``.`` Not Running\n\n"
                   "(2) ``M``   Monitored,         ``.`` Not Monitored\n\n"
                   "(3) ``D``   Disabled,          ``.`` Enabled\n\n"
                   "(4) ``O``   Optional,          ``.`` Not Optional\n\n"
                   "(5) ``E``   Encap,             ``.`` Not Encap\n\n"
                   "(6) ``P``   Not Provisioned,   ``.`` Provisioned\n\n"
                   "(7) ``S``   Standby,           ``.`` Not Standby\n\n"
                   "(8) ``<n>`` Remaining Restart, ``+`` if more than 10,  ``.``   No Restart\n\n"
                   "",
            "options": [
                OPT.filter,
                OPT.format,
                OPT.hide_disabled,
                OPT.refresh,
                OPT.show_disabled,
            ],
        },
        "print_config_mtime": {
            "msg": "Display the service local configuration file modification time",
        },
        "purge": {
            "msg": "Unprovision and delete selected services.",
            "options": ASYNC_ACTION_OPTS + ACTION_OPTS + [
                OPT.purge_collector,
                OPT.leader,
            ],
        },
        "status": {
            "msg": "Return the local service instance overall status code.",
            "options": [
                OPT.cron,
                OPT.refresh,
            ],
        },
    },
    "Object configuration": {
        "print_config": {
            "msg": "Display the service current configuration.",
            "options": [
                OPT.filter,
                OPT.format,
                OPT.eval,
                OPT.impersonate,
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
            "options": CONFIG_OPTS + [
                OPT.config,
                OPT.disable_rollback,
                OPT.kw,
                OPT.interactive,
                OPT.leader,
                OPT.provision,
                OPT.resource,
                OPT.restore,
                OPT.template,
            ],
        },
        "update": {
            "msg": "Update definitions in an existing service configuration "
                   "file.",
            "options": CONFIG_OPTS + [
                OPT.disable_rollback,
                OPT.provision,
                OPT.resource,
            ],
        },
        "delete": {
            "msg": "Delete a service, or only the resources specified by :opt:`--rid` on the local service instance.",
            "options": ASYNC_ACTION_OPTS + CONFIG_OPTS + [
                OPT.purge_collector,
                OPT.unprovision,
            ],
        },
        "eval": {
            "msg": "Evaluate the value of a service configuration keyword.",
            "options": CONFIG_OPTS + [
                OPT.format,
                OPT.impersonate,
                OPT.kw,
            ],
        },
        "set": {
            "msg": "Set a service configuration parameter",
            "options": CONFIG_OPTS + [
                OPT.kw,
                OPT.add,
                OPT.eval,
                OPT.index,
                OPT.param,
                OPT.remove,
                OPT.value,
            ],
        },
        "get": {
            "msg": "Get the raw value of a service configuration keyword.",
            "options": CONFIG_OPTS + [
                OPT.eval,
                OPT.format,
                OPT.impersonate,
                OPT.param,
                OPT.kw,
            ],
        },
        "unset": {
            "msg": "Unset a node configuration keyword.",
            "options": CONFIG_OPTS + [
                OPT.kw,
                OPT.param,
            ],
        },
    },
}
