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
        help="decides that the unavailabity period should be "
             "deduced from the service availability anyway. "
             "used with the 'collector ack unavailability' "
             "action"),
    "attach": Option(
        "--attach", default=False,
        action="store_true", dest="attach",
        help="attach the modulesets specified during a "
             "compliance check/fix/fixable command"),
    "author": Option(
        "--author", default=None,
        action="store", dest="author",
        help="the acker name to log when used with the "
             "'collector ack unavailability' action"),
    "begin": Option(
        "--begin", default=None,
        action="store", dest="begin",
        help="a begin date expressed as 'YYYY-MM-DD hh:mm'. "
             "used with the 'collector ack unavailability' "
             "action"),
    "cluster": Option(
        "-c", "--cluster", default=False,
        action="store_true", dest="cluster",
        help="option to set when excuting from a clusterware to"
             " disable safety net"),
    "color": Option(
        "--color", default="auto",
        action="store", dest="color",
        help="colorize output. possible values are : auto=guess "
             "based on tty presence, always|yes=always colorize, "
             "never|no=never colorize"),
    "comment": Option(
        "--comment", default=None,
        action="store", dest="comment",
        help="a comment to log when used with the 'collector "
             "ack unavailability' action"),
    "config": Option(
        "--config", default=None,
        action="store", dest="parm_config",
        help="the configuration file to use when creating or "
             "installing a service"),
    "cron": Option(
        "--cron", default=False,
        action="store_true", dest="cron",
        help="used by cron'ed action to tell the collector to "
             "treat the log entries as such"),
    "debug": Option(
        "--debug", default=False,
        action="store_true", dest="debug",
        help="debug mode"),
    "daemon": Option(
        "--daemon", default=False,
        action="store_true", dest="daemon",
        help="a flag inhibiting the daemonization. set by the "
             "daemonization routine."),
    "disable_rollback": Option(
        "--disable-rollback", default=False,
        action="store_true", dest="disable_rollback",
        help="Exit without resource activation rollback on start"
             " action error"),
    "discard": Option(
        "--discard", default=False,
        action="store_true", dest="discard",
        help="Discard the stashed erroneous configuration file "
             "in a 'edit config' command"),
    "dry_run": Option(
        "--dry-run", default=False,
        action="store_true", dest="dry_run",
        help="Show the action execution plan"),
    "duration": Option(
        "--duration", default=None,
        action="store", dest="duration", type="int",
        help="a duration expressed in minutes. used with the "
             "'collector ack unavailability' action"),
    "end": Option(
        "--end", default=None,
        action="store", dest="end",
        help="a end date expressed as 'YYYY-MM-DD hh:mm'. used "
             "with the 'collector ack unavailability' action"),
    "env": Option(
        "--env", default=[],
        action="append", dest="env",
        help="with the create action, set a env section "
             "parameter. multiple --env <key>=<val> can be "
             "specified."),
    "eval": Option(
        "--eval", default=False,
        action="store_true", dest="eval",
        help="If set with the 'get' action, the printed value of "
             "--param is scoped and dereferenced."),
    "force": Option(
        "-f", "--force", default=False,
        action="store_true", dest="force",
        help="force action, ignore sanity check warnings"),
    "format": Option(
        "--format", default=None,
        action="store", dest="format",
        help="specify a data formatter for output of the print*"
             " and collector* commands. possible values are json"
             " or table."),
    "help": Option(
        "-h", "--help", default=None,
        action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "hide_disabled": Option(
        "--hide-disabled", default=None,
        action="store_false", dest="show_disabled",
        help="tell print|json status action to not include the "
             "disabled resources in the output, irrespective of"
             " the show_disabled service configuration setting."),
    "id": Option(
        "--id", default=0,
        action="store", dest="id", type="int",
        help="specify an object id to act on"),
    "ignore_affinity": Option(
        "--ignore-affinity", default=False,
        action="store_true", dest="ignore_affinity",
        help="ignore service anti-affinity with other services "
             "check"),
    "interactive": Option(
        "-i", "--interactive", default=False,
        action="store_true", dest="interactive",
        help="prompt user for a choice instead of going for "
             "defaults or failing"),
    "like": Option(
        "--like", default="%",
        action="store", dest="like",
        help="a sql like filtering expression. leading and "
             "trailing wildcards are automatically set."),
    "master": Option(
        "--master", default=False,
        action="store_true", dest="master",
        help="option to set to limit the action scope to the "
             "master service resources"),
    "message": Option(
        "--message", default="",
        action="store", dest="message",
        help="the message to send to the collector for logging"),
    "module": Option(
        "--module", default="",
        action="store", dest="module",
        help="compliance, set module list"),
    "moduleset": Option(
        "--moduleset", default="",
        action="store", dest="moduleset",
        help="compliance, set moduleset list. The 'all' value "
             "can be used in conjonction with detach."),
    "onlyprimary": Option(
        "--onlyprimary", default=None,
        action="store_true", dest="parm_primary",
        help="operate only on service flagged for autostart on "
             "this node"),
    "onlysecondary": Option(
        "--onlysecondary", default=None,
        action="store_true", dest="parm_secondary",
        help="operate only on service not flagged for autostart"
             " on this node"),
    "parallel": Option(
        "-p", "--parallel", default=False,
        action="store_true", dest="parallel",
        help="start actions on specified services in parallel"),
    "param": Option(
        "--param", default=None,
        action="store", dest="param",
        help="point a service configuration parameter for the "
             "'get' and 'set' actions"),
    "provision": Option(
        "--provision", default=False,
        action="store_true", dest="provision",
        help="with the install or create actions, provision the"
             " service resources after config file creation. "
             "defaults to False."),
    "recover": Option(
        "--recover", default=False,
        action="store_true", dest="recover",
        help="Recover the stashed erroneous configuration file "
             "in a 'edit config' command"),
    "refresh": Option(
        "--refresh", default=False,
        action="store_true", dest="refresh",
        help="drop last resource status cache and re-evaluate "
             "before printing with the 'print [json] status' "
             "commands"),
    "remote": Option(
        "--remote", default=False,
        action="store_true", dest="remote",
        help="flag action as triggered by a remote node. used "
             "to avoid recursively triggering actions amongst "
             "nodes"),
    "resource": Option(
        "--resource", default=[],
        action="append",
        help="a resource definition in json dictionary format "
             "fed to create or update"),
    "rid": Option(
        "--rid", default=None,
        action="store", dest="parm_rid",
        help="comma-separated list of resource to limit action "
             "to"),
    "ruleset": Option(
        "--ruleset", default="",
        action="store", dest="ruleset",
        help="compliance, set ruleset list. The 'all' value can"
             " be used in conjonction with detach."),
    "ruleset_date": Option(
        "--ruleset-date", default="",
        action="store", dest="ruleset_date",
        help="compliance, use rulesets valid on specified date"),
    "service": Option(
        "-s", "--service", default=None,
        action="store", dest="parm_svcs",
        help="comma-separated list of service to operate on"),
    "show_disabled": Option(
        "--show-disabled", default=None,
        action="store_true", dest="show_disabled",
        help="tell print|json status action to include the "
             "disabled resources in the output, irrespective of"
             " the show_disabled service configuration setting."),
    "slave": Option(
        "--slave", default=None, action="store", dest="slave",
        help="option to set to limit the action scope to the "
             "service resources in the specified, comma-"
             "separated, slaves"),
    "slaves": Option(
        "--slaves", default=False,
        action="store_true", dest="slaves",
        help="option to set to limit the action scope to all "
             "slave service resources"),
    "status": Option(
        "--status", default=None,
        action="store", dest="parm_status",
        help="operate only on service in the specified status "
             "(up/down/warn)"),
    "subsets": Option(
        "--subsets", default=None,
        action="store", dest="parm_subsets",
        help="comma-separated list of resource subsets to limit"
             " action to"),
    "tag": Option(
        "--tag", default=None,
        action="store", dest="tag",
        help="a tag specifier used by 'collector create tag', "
             "'collector add tag', 'collector del tag'"),
    "tags": Option(
        "--tags", default=None,
        action="store", dest="parm_tags",
        help="comma-separated list of resource tags to limit "
             "action to. The + separator can be used to impose "
             "multiple tag conditions. Example: tag1+tag2,tag3 "
             "limits the action to resources with both tag1 and"
             " tag2, or tag3."),
    "template": Option(
        "--template", default=None,
        action="store", dest="parm_template",
        help="the configuration file template name or id, "
             "served by the collector, to use when creating or "
             "installing a service"),
    "to": Option(
        "--to", default=None,
        action="store", dest="parm_destination_node",
        help="remote node to start or migrate the service to"),
    "unprovision": Option(
        "--unprovision", default=False,
        action="store_true", dest="unprovision",
        help="with the delete action, unprovision the service "
             "resources before config files file deletion. "
             "defaults to False."),
    "value": Option(
        "--value", default=None,
        action="store", dest="value",
        help="set a service configuration parameter value for "
             "the 'set --param' action"),
    "verbose": Option(
        "--verbose", default=False,
        action="store_true", dest="verbose",
        help="add more information to some print commands: +next"
             " in 'print schedule'"),
    "waitlock": Option(
        "--waitlock", default=-1,
        action="store", dest="parm_waitlock", type="int",
        help="comma-separated list of resource tags to limit "
             "action to"),
})

SVCMGR_OPTS = [
    OPT.onlyprimary,
    OPT.onlysecondary,
    OPT.service,
    OPT.status,
]

GLOBAL_OPTS = SVCMGR_OPTS + [
    OPT.cluster,
    OPT.color,
    OPT.cron,
    OPT.daemon,
    OPT.debug,
    OPT.parallel,
    OPT.waitlock,
    OPT.help,
    OPT.remote,
]

ACTION_OPTS = [
    OPT.dry_run,
    OPT.force,
    OPT.master,
    OPT.rid,
    OPT.slave,
    OPT.slaves,
    OPT.subsets,
    OPT.tags,
]

START_ACTION_OPTS = ACTION_OPTS + [
    OPT.disable_rollback,
    OPT.ignore_affinity,
]

ACTIONS = {
    'Service actions': {
        'boot': {
            'msg': 'start a service if executed on the primary node (or one of'
                   ' the primary nodes in case of a flex service), '
                   'startstandby if not',
            'options': ACTION_OPTS,
        },
        'dns_update': {
            'msg': 'update the collector dns records for the service',
            'options': ACTION_OPTS,
        },
        'shutdown': {
            'msg': 'stop a service, disabling the background database logging',
            'options': ACTION_OPTS,
        },
        'start': {
            'msg': 'start all service resources',
            'options': ACTION_OPTS,
        },
        'startstandby': {
            'msg': 'start service resources flagged always on',
            'options': ACTION_OPTS,
        },
        'startip': {
            'msg': 'configure service ip addresses',
            'options': ACTION_OPTS,
        },
        'startshare': {
            'msg': 'start network shares',
            'options': ACTION_OPTS,
        },
        'stopshare': {
            'msg': 'stop network shares',
            'options': ACTION_OPTS,
        },
        'startfs': {
            'msg': 'prepare devices, logical volumes, mount service '
                   'filesystems, bootstrap containers',
            'options': ACTION_OPTS,
        },
        'startapp': {
            'msg': 'execute service application startup script',
            'options': ACTION_OPTS,
        },
        'stop': {
            'msg': 'stop all service resources not flagged always on. With '
                   '--force, stop all service resources, even those flagged '
                   'always on.',
            'options': ACTION_OPTS,
        },
        'stopip': {
            'msg': 'unconfigure service ip addresses',
            'options': ACTION_OPTS,
        },
        'stopfs': {
            'msg': 'shutdown container, umount service filesystems, deactivate'
                   ' logical volumes',
            'options': ACTION_OPTS,
        },
        'stopapp': {
            'msg': 'execute service application stop script',
            'options': ACTION_OPTS,
        },
        'startcontainer': {
            'msg': 'start the container resource',
            'options': ACTION_OPTS,
        },
        'stopcontainer': {
            'msg': 'stop the container resource',
            'options': ACTION_OPTS,
        },
        'provision': {
            'msg': 'provision and start the service',
            'options': ACTION_OPTS,
        },
        'unprovision': {
            'msg': 'stop and unprovision the service. beware: data will be '
                   'lost upon fs and disk unprovisioning.',
            'options': ACTION_OPTS,
        },
        'disable': {
            'msg': 'disable resources passed through --rid in services passed'
                   ' through --service. Specifying no resource disables the '
                   'whole service.',
            'options': [
                OPT.rid,
                OPT.tags,
                OPT.subsets,
            ],
        },
        'enable': {
            'msg': 'enable resources passed through --rid in services passed'
                   ' through --service. Specifying no resource enables the '
                   'whole service.',
            'options': [
                OPT.rid,
                OPT.tags,
                OPT.subsets,
            ],
        },
        'status': {
            'msg': 'return service overall status code',
            'options': [
                OPT.refresh,
            ],
        },
        'print_status': {
            'msg': 'display service resource status',
            'options': [
                OPT.format,
                OPT.hide_disabled,
                OPT.refresh,
                OPT.show_disabled,
            ],
        },
        'print_resource_status': {
            'msg': 'display a specific service resource status, pointed by'
                   ' --rid',
            'options': [
                OPT.format,
                OPT.refresh,
                OPT.rid,
            ],
        },
        'print_config_mtime': {
            'msg': 'display service configuration file modification time',
        },
        'freeze': {
            'msg': 'set up a flag to block actions on this service',
        },
        'thaw': {
            'msg': 'remove the flag to unblock actions on this service',
        },
        'frozen': {
            'msg': 'report on the current blocking of actions on this service',
        },
        'run': {
            'msg': 'run all tasks, or tasks specified by --rid --tags and '
                   '--subset, disregarding their schedule',
            'options': ACTION_OPTS,
        },
        'startdisk': {
            'msg': 'combo action, activating standby disks, taking '
                   'reservations, starting loopback devices and volume '
                   'groups',
            'options': ACTION_OPTS,
        },
        'stopdisk': {
            'msg': 'combo action, stopping volume groups and loopback '
                   'devices, droping reservations, disabling standby disks',
            'options': ACTION_OPTS,
        },
        'presync': {
            'msg': 'update var files associated to resources',
            'options': ACTION_OPTS,
        },
        'postsync': {
            'msg': 'make use of files received from master nodes in var',
            'options': ACTION_OPTS,
        },
        'prstart': {
            'msg': 'reserve scsi disks held by this service',
            'options': ACTION_OPTS,
        },
        'prstop': {
            'msg': 'release scsi disks held by this service',
            'options': ACTION_OPTS,
        },
        'prstatus': {
            'msg': 'report status of reservations on scsi disks held by this '
                   'service',
        },
        'restart': {
            'msg': 'combo action, chaining stop-start',
            'options': ACTION_OPTS,
        },
        'resync': {
            'msg': 'combo action, chaining stop-sync_resync-start',
            'options': ACTION_OPTS,
        },
        'sync_nodes': {
            'msg': 'send to peer nodes the service config files and '
                   'additional files described in the config file.',
            'options': ACTION_OPTS,
        },
        'sync_drp': {
            'msg': 'send to drp nodes the service config files and '
                   'additional files described in the config file.',
            'options': ACTION_OPTS,
        },
        'sync_quiesce': {
            'msg': 'trigger a storage hardware-assisted disk synchronization',
            'options': ACTION_OPTS,
        },
        'sync_break': {
            'msg': 'split a storage hardware-assisted disk synchronization',
            'options': ACTION_OPTS,
        },
        'sync_split': {
            'msg': 'split a EMC SRDF storage hardware-assisted disk '
                   'synchronization',
            'options': ACTION_OPTS,
        },
        'sync_establish': {
            'msg': 'establish a EMC SRDF storage hardware-assisted disk '
                   'synchronization',
            'options': ACTION_OPTS,
        },
        'sync_resync': {
            'msg': 'like sync_update, but not triggered by the scheduler '
                   '(thus adapted for clone/snap operations)',
            'options': ACTION_OPTS,
        },
        'sync_full': {
            'msg': 'trigger a full copy of the volume to its target',
            'options': ACTION_OPTS,
        },
        'sync_restore': {
            'msg': 'trigger a restore of the sync resources data to their '
                   'target path (DANGEROUS: make sure you understand before '
                   'running this action).',
            'options': ACTION_OPTS,
        },
        'sync_update': {
            'msg': 'trigger a one-time resync of the volume to its target',
            'options': ACTION_OPTS,
        },
        'sync_resume': {
            'msg': 're-establish a broken storage hardware-assisted '
                   'synchronization',
            'options': ACTION_OPTS,
        },
        'sync_revert': {
            'msg': 'revert to the pre-failover data (looses current data)',
            'options': ACTION_OPTS,
        },
        'sync_verify': {
            'msg': 'trigger a one-time checksum-based verify of the volume '
                   'and its target',
            'options': ACTION_OPTS,
        },
        'sync_all': {
            'msg': 'combo action, chaining sync_nodes-sync_drp-sync_update.',
            'options': ACTION_OPTS,
        },
        'push': {
            'msg': 'push service configuration to the collector',
        },
        'pull': {
            'msg': 'pull a service configuration from the collector',
            'options': [
                OPT.provision,
            ],
        },
        'push_resinfo': {
            'msg': 'push service resources and application launchers info '
                   'key/value pairs the collector',
        },
        'push_service_status': {
            'msg': 'push service and its resources status to database',
        },
        'print_disklist': {
            'msg': 'print service disk list',
            'options': [
                OPT.format,
            ],
        },
        'print_devlist': {
            'msg': 'print service device list',
            'options': [
                OPT.format,
            ],
        },
        'switch': {
            'msg': 'stop the service on the local node and start on the '
                   'remote node. --to <node> specify the remote node to '
                   'switch the service to.',
            'options': ACTION_OPTS + [
                OPT.to,
            ],
        },
        'migrate': {
            'msg': 'live migrate the service to the remote node. '
                   '--to <node> specify the remote node to migrate the '
                   'service to.',
            'options': ACTION_OPTS + [
                OPT.to,
            ],
        },
        'resource_monitor': {
            'msg': 'detect monitored resource failures and trigger '
                   'monitor_action',
            'options': ACTION_OPTS,
        },
        'stonith': {
            'msg': 'command provided to the heartbeat daemon to fence peer '
                   'node in case of split brain',
            'options': ACTION_OPTS,
        },
        'docker': {
            'msg': 'wrap the docker client command, setting automatically '
                   'the socket parameter to join the service-private docker '
                   'daemon. The %as_service%, %images% and %instances% words '
                   'in the wrapped command are replaced by, respectively, '
                   'the registry login username/password/email parameters to '
                   'log as a service using <svcname>@<nodename> as the '
                   'username and the node uuid as password (which is what '
                   'is expected when the opensvc collector is used as the '
                   'JWT manager for the registry), the set of docker '
                   'instance names and images for container resources '
                   'passing the --tags, --rid and --subsets filters. This is '
                   'useful to remove all instances of a service or all '
                   'instances of resources with a tag like "frontend". Note '
                   'the opensvc filters must be positioned before the docker '
                   'command in the arguments list.',
        },
        'print_schedule': {
            'msg': 'print the service tasks schedule',
            'options': [
                OPT.format,
                OPT.verbose,
            ],
        },
        'scheduler': {
            'msg': 'run the service task scheduler',
        },
        'pg_freeze': {
            'msg': 'freeze the tasks of a process group',
            'options': ACTION_OPTS,
        },
        'pg_thaw': {
            'msg': 'thaw the tasks of a process group',
            'options': ACTION_OPTS,
        },
        'pg_kill': {
            'msg': 'kill the tasks of a process group',
            'options': ACTION_OPTS,
        },
        'logs': {
            'msg': 'display the service logs in the pager',
        },
    },
    'Service configuration': {
        'print_config': {
            'msg': 'display service current configuration',
            'options': [
                OPT.format,
            ],
        },
        'edit_config': {
            'msg': 'edit service configuration',
            'options': [
                OPT.discard,
                OPT.recover,
            ],
        },
        'validate_config': {
            'msg': 'check the sections and parameters are valid.',
        },
        'create': {
            'msg': 'create a new service configuration file. --interactive '
                   'triggers the interactive mode. --template <template '
                   'name>|<template id>|<uri>|<local path> fetchs and '
                   'installs a service config template. --config <uri>|<local'
                   ' path> fetchs and installs a service config file. '
                   '--provision create the system resources defined in the '
                   'service config.',
            'options': ACTION_OPTS + [
                OPT.config,
                OPT.env,
                OPT.interactive,
                OPT.provision,
                OPT.resource,
                OPT.template,
            ],
        },
        'update': {
            'msg': 'update definitions in an existing service configuration '
                   'file',
            'options': ACTION_OPTS + [
                OPT.interactive,
                OPT.provision,
                OPT.resource,
            ],
        },
        'delete': {
            'msg': 'delete the service instance on the local node if no '
                   '--rid is specified, or delete the resources pointed by '
                   '--rid in services passed through --service',
            'options': ACTION_OPTS + [
                OPT.unprovision,
            ],
        },
        'set': {
            'msg': 'set a service configuration parameter',
            'options': [
                OPT.param,
                OPT.value,
            ],
        },
        'get': {
            'msg': 'get the raw or dereferenced value of a service '
                   'configuration parameter',
            'options': [
                OPT.eval,
                OPT.param,
            ],
        },
        'unset': {
            'msg': 'unset a node configuration parameter pointed by --param',
            'options': [
                OPT.param,
            ],
        },
    },
    'Compliance': {
        'compliance_auto': {
            'msg': 'run compliance checks or fixes depending on the autofix'
                   'module property values.',
            'options': [
                OPT.attach,
                OPT.force,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        'compliance_check': {
            'msg': 'run compliance checks.',
            'options': [
                OPT.attach,
                OPT.force,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        'compliance_fix': {
            'msg': 'run compliance fixes.',
            'options': [
                OPT.attach,
                OPT.force,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        'compliance_fixable': {
            'msg': 'verify compliance fixes prerequisites.',
            'options': [
                OPT.attach,
                OPT.force,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        'compliance_env': {
            'msg': 'show the compliance modules environment variables.',
            'options': [
                OPT.module,
                OPT.moduleset,
            ],
        },
        'compliance_show_status': {
            'msg': 'show compliance modules status',
        },
        'compliance_show_moduleset': {
            'msg': 'show compliance rules applying to this service',
        },
        'compliance_list_moduleset': {
            'msg': 'list available compliance modulesets. --moduleset f% '
                   'limit the scope to modulesets matching the f% pattern.',
        },
        'compliance_attach_moduleset': {
            'msg': 'attach moduleset specified by --moduleset to this service',
            'options': [
                OPT.moduleset,
            ],
        },
        'compliance_detach_moduleset': {
            'msg': 'detach moduleset specified by --moduleset from this '
                   'service',
            'options': [
                OPT.moduleset,
            ],
        },
        'compliance_list_ruleset': {
            'msg': 'list available compliance rulesets. --ruleset f% limit '
                   'the scope to rulesets matching the f% pattern.',
        },
        'compliance_show_ruleset': {
            'msg': 'show compliance rules applying to this node',
        },
        'compliance_attach_ruleset': {
            'msg': 'attach ruleset specified by --ruleset to this service',
            'options': [
                OPT.ruleset,
            ],
        },
        'compliance_detach_ruleset': {
            'msg': 'detach ruleset specified by --ruleset from this service',
            'options': [
                OPT.ruleset,
            ],
        },
        'compliance_attach': {
            'msg': 'attach ruleset specified by --ruleset and/or moduleset '
                   'specified by --moduleset to this service',
            'options': [
                OPT.moduleset,
                OPT.ruleset,
            ],
        },
        'compliance_detach': {
            'msg': 'detach ruleset specified by --ruleset and/or moduleset '
                   'specified by --moduleset from this service',
            'options': [
                OPT.moduleset,
                OPT.ruleset,
            ],
        },
    },
    'Collector management': {
        'collector_ack_unavailability': {
            'msg': 'acknowledge an unavailability period. the period is '
                   'specified by --begin/--end or --begin/--duration. '
                   'omitting --begin defaults to now. an acknowlegment can '
                   'be completed by --author (defaults to root@nodename), '
                   '--account (default to 1) and --comment',
            'options': [
                OPT.author,
                OPT.account,
                OPT.begin,
                OPT.end,
                OPT.comment,
                OPT.duration,
            ],
        },
        'collector_list_unavailability_ack': {
            'msg': 'list acknowledged periods for the service. the periods '
                   'can be filtered by --begin/--end. omitting --end '
                   'defaults to now. the wildcard for --comment and '
                   '--author is %',
            'options': [
                OPT.author,
                OPT.begin,
                OPT.end,
                OPT.comment,
            ],
        },
        'collector_list_actions': {
            'msg': 'list actions on the service, whatever the node, during '
                   'the period specified by --begin/--end. --end defaults to '
                   'now. --begin defaults to 7 days ago',
            'options': [
                OPT.begin,
                OPT.end,
                OPT.format,
            ],
        },
        'collector_ack_action': {
            'msg': 'acknowledge an action error on the service. an '
                   'acknowlegment can be completed by --author (defaults '
                   'to root@nodename) and --comment',
            'options': [
                OPT.author,
                OPT.comment,
            ],
        },
        'collector_show_actions': {
            'msg': 'show actions detailed log. a single action is specified '
                   'by --id. a range is specified by --begin/--end dates. '
                   '--end defaults to now. --begin defaults to 7 days ago',
            'options': [
                OPT.begin,
                OPT.id,
                OPT.end,
                OPT.format,
            ],
        },
        'collector_checks': {
            'msg': 'display service checks',
            'options': [
                OPT.format,
            ],
        },
        'collector_disks': {
            'msg': 'display service disks',
            'options': [
                OPT.format,
            ],
        },
        'collector_log': {
            'msg': 'log a message in the collector\'s service log',
            'options': [
                OPT.message,
            ],
        },
        'collector_alerts': {
            'msg': 'display service alerts',
            'options': [
                OPT.format,
            ],
        },
        'collector_events': {
            'msg': 'display service events during the period specified by '
                   '--begin/--end. --end defaults to now. --begin defaults '
                   'to 7 days ago',
            'options': [
                OPT.begin,
                OPT.end,
                OPT.format,
            ],
        },
        'collector_asset': {
            'msg': 'display asset information known to the collector',
            'options': [
                OPT.format,
            ],
        },
        'collector_networks': {
            'msg': 'display network information known to the collector for '
                   'each service ip',
            'options': [
                OPT.format,
            ],
        },
        'collector_tag': {
            'msg': 'set a service tag (pointed by --tag)',
            'options': [
                OPT.tag,
            ],
        },
        'collector_untag': {
            'msg': 'unset a service tag (pointed by --tag)',
            'options': [
                OPT.tag,
            ],
        },
        'collector_show_tags': {
            'msg': 'list all service tags',
            'options': [
                OPT.format,
            ],
        },
        'collector_list_tags': {
            'msg': 'list all available tags. use --like to filter the output.',
            'options': [
                OPT.format,
                OPT.like,
            ],
        },
        'collector_create_tag': {
            'msg': 'create a new tag',
            'options': [
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
    "json_devlist",
    "json_disklist",
    "json_env",
    "json_schedule",
    "json_status",
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

class SvcmgrOptParser(OptParser):
    """
    The svcmgr-specific options parser class
    """
    def __init__(self, args=None, colorize=True, width=None, formatter=None,
                 indent=6):
        OptParser.__init__(self, args=args, prog=PROG, options=OPT,
                           actions=ACTIONS,
                           deprecated_actions=DEPRECATED_ACTIONS,
                           global_options=GLOBAL_OPTS,
                           svcmgr_options=SVCMGR_OPTS,
                           colorize=colorize, width=width,
                           formatter=formatter, indent=indent)

