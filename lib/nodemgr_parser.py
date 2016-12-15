"""
svcmgr command line actions and options
"""
from rcGlobalEnv import Storage
from rcOptParser import OptParser
from optparse import Option

PROG = "nodemgr"

OPT = Storage({
    "api": Option(
        "--api", default=None, action="store", dest="api",
        help="specify a collector api url different from the "
             "one set in node.conf. Honored by the 'collector "
             "cli' action."),
    "app": Option(
        "--app", default=None, action="store", dest="app",
        help="Optional with the register command, register the "
             "node in the specified app. If not specified, the "
             "node is registered in the first registering "
             "user's app found."),
    "attach": Option(
        "--attach", default=False,
        action="store_true", dest="attach",
        help="attach the modulesets specified during a "
             "compliance check/fix/fixable command"),
    "author": Option(
        "--author", default=None,
        action="store", dest="author",
        help="the acker name to log when used with the "
             "'collector ack action' action"),
    "begin": Option(
        "--begin", default=None,
        action="store", dest="begin",
        help="a begin date expressed as 'YYYY-MM-DD hh:mm'. "
             "used with the 'collector ack action' and pushstats "
             "action"),
    "broadcast": Option(
        "--broadcast", default=None,
        action="store", dest="broadcast",
        help="list of broadcast addresses, comma separated, "
             "used by the 'wol' action"),
    "color": Option(
        "--color", default="auto",
        action="store", dest="color",
        help="colorize output. possible values are : auto=guess "
             "based on tty presence, always|yes=always colorize,"
             " never|no=never colorize"),
    "comment": Option(
        "--comment", default=None,
        action="store", dest="comment",
        help="a comment to log when used with the 'collector ack "
             "action' action"),
    "config": Option(
        "--config", default=None, action="store", dest="config",
        help="specify a user-specific collector api connection "
             "configuration file. defaults to '~/.opensvc-cli'. "
             "Honored by the 'collector cli' action."),
    "cron": Option(
        "--cron", default=False,
        action="store_true", dest="cron",
        help="cron mode"),
    "debug": Option(
        "--debug", default=False,
        action="store_true", dest="debug",
        help="debug mode"),
    "duration": Option(
        "--duration", default=None,
        action="store", dest="duration", type="int",
        help="a duration expressed in minutes. used with the "
             "'collector ack action' action"),
    "end": Option(
        "--end", default=None,
        action="store", dest="end",
        help="a end date expressed as 'YYYY-MM-DD hh:mm'. used "
             "with the 'collector ack action' and pushstats "
             "action"),
    "filterset": Option(
        "--filterset", default="",
        action="store", dest="filterset",
        help="set a filterset to limit collector extractions"),
    "force": Option(
        "--force", default=False,
        action="store_true", dest="force",
        help="force action"),
    "format": Option(
        "--format", default=None, action="store", dest="format",
        help="specify a data formatter for output of the print* "
             "and collector* commands. possible values are json "
             "or table."),
    "help": Option(
        "-h", "--help", default=None,
        action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "id": Option(
        "--id", default=0,
        action="store", dest="id", type="int",
        help="specify an id to act on"),
    "insecure": Option(
        "--insecure", default=False,
        action="store_true", dest="insecure",
        help="allow communications with a collector presenting "
             "unverified SSL certificates."),
    "like": Option(
        "--like", default="%",
        action="store", dest="like",
        help="a sql like filtering expression. leading and "
             "trailing wildcards are automatically set."),
    "mac": Option(
        "--mac", default=None,
        action="store", dest="mac",
        help="list of mac addresses, comma separated, used by "
             "the 'wol' action"),
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
    "opt_object": Option(
        "--object", default=[], action="append", dest="objects",
        help="an object to limit a push* action to. multiple "
             "--object <object id> parameters can be set on a "
             "single command line"),
    "param": Option(
        "--param", default=None,
        action="store", dest="param",
        help="point a node configuration parameter for the 'get'"
             " and 'set' actions"),
    "password": Option(
        "--password", default=None,
        action="store", dest="password",
        help="authenticate with the collector using the "
             "specified user credentials instead of the node "
             "credentials. Prompted if necessary but not "
             "specified."),
    "refresh_api": Option(
        "--refresh-api", default=False,
        action="store_true", dest="refresh_api",
        help="The OpenSVC collector api url"),
    "resource": Option(
        "--resource", default=[],
        action="append",
        help="a resource definition in json dictionary format "
             "fed to the provision action"),
    "ruleset": Option(
        "--ruleset", default="",
        action="store", dest="ruleset",
        help="compliance, set ruleset list. The 'all' value can "
             "be used in conjonction with detach."),
    "ruleset_date": Option(
        "--ruleset-date", default="",
        action="store", dest="ruleset_date",
        help="compliance, use rulesets valid on specified date"),
    "stats_dir": Option(
        "--stats-dir", default=None,
        action="store", dest="stats_dir",
        help="points the directory where the metrics files are "
             "stored for pushstats"),
    "symcli_db_file": Option(
        "--symcli-db-file", default=None,
        action="store", dest="symcli_db_file",
        help="[pushsym option] use symcli offline mode with the "
             "specified file. aclx files are expected to be "
             "found in the same directory and named either "
             "<symid>.aclx or <same_prefix_as_bin_file>.aclx"),
    "sync": Option(
        "--sync", default=False,
        action="store_true", dest="syncrpc",
        help="use synchronous collector rpc if available. to "
             "use with pushasset when chaining a compliance "
             "run, to make sure the node ruleset is "
             "up-to-date."),
    "tag": Option(
        "--tag", default=None,
        action="store", dest="tag",
        help="a tag specifier used by 'collector create tag', "
             "'collector add tag', 'collector del tag'"),
    "user": Option(
        "--user", default=None, action="store", dest="user",
        help="authenticate with the collector using the "
             "specified user credentials instead of the node "
             "credentials. Required for the 'register' action "
             "when the collector is configured to refuse "
             "anonymous register."),
    "value": Option(
        "--value", default=None,
        action="store", dest="value",
        help="set a node configuration parameter value for the "
             "'set --param' action"),
    "verbose": Option(
        "--verbose", default=False,
        action="store_true", dest="verbose",
        help="add more information to some print commands: +next "
             "in 'print schedule'"),
})

GLOBAL_OPTS = [
    OPT.color,
    OPT.cron,
    OPT.debug,
    OPT.format,
    OPT.help,
]

ACTIONS = {
    'Node actions': {
        'logs': {
            'msg': 'fancy display of the node logs',
        },
        'shutdown': {
            'msg': 'shutdown the node to powered off state',
        },
        'reboot': {
            'msg': 'reboot the node',
        },
        'scheduler': {
            'msg': 'run the node task scheduler',
        },
        'schedulers': {
            'msg': 'execute a run of the node and services schedulers. this '
                   'action is installed in the system scheduler',
        },
        'schedule_reboot_status': {
            'msg': 'tell if the node is scheduled for reboot',
        },
        'schedule_reboot': {
            'msg': 'mark the node for reboot at the next allowed period. the '
                   'allowed period is defined by a "reboot" section in '
                   'node.conf.',
        },
        'unschedule_reboot': {
            'msg': 'unmark the node for reboot at the next allowed period.',
        },
        'provision': {
            'msg': 'provision the resources described in --resource arguments',
            'options': [
                OPT.resource,
            ],
        },
        'updatepkg': {
            'msg': 'upgrade the opensvc agent version. the packages must be '
                   'available behind the node.repo/packages url.',
        },
        'updatecomp': {
            'msg': 'upgrade the opensvc compliance modules. the modules must '
                   'be available as a tarball behind the node.repo/compliance '
                   'url.',
        },
        'scanscsi': {
            'msg': 'scan the scsi hosts in search of new disks',
        },
        'dequeue_actions': {
            'msg': "dequeue and execute actions from the collector's action "
                   "queue for this node and its services.",
        },
        'rotate_root_pw': {
            'msg': "set a new root password and store it in the collector",
        },
        'print_schedule': {
            'msg': 'print the node tasks schedule',
            'options': [
                OPT.verbose,
            ],
        },
        'wol': {
            'msg': 'forge and send udp wake on lan packet to mac address '
                   'specified by --mac and --broadcast arguments',
            'options': [
                OPT.broadcast,
                OPT.mac,
            ],
        },
        'collect_stats': {
            'msg': "write in local files metrics not found in the standard "
                   "metrics collector. these files will be fed to the "
                   "collector by the 'pushstat' action.",
        },
    },
    'Service actions': {
        'discover': {
            'msg': 'discover vservices accessible from this host, cloud nodes for example',
        },
    },
    'Node configuration': {
        'print_config': {
            'msg': 'open the node.conf configuration file with the preferred editor',
        },
        'print_authconfig': {
            'msg': 'open the node.conf configuration file with the preferred editor',
        },
        'edit_config': {
            'msg': 'open the node.conf configuration file with the preferred editor',
        },
        'edit_authconfig': {
            'msg': 'open the auth.conf configuration file with the preferred editor',
        },
        'register': {
            'msg': 'obtain a registration number from the collector, used to authenticate the node',
            'options': [
                OPT.app,
                OPT.password,
                OPT.user,
            ],
        },
        'get': {
            'msg': 'get the value of the node configuration parameter pointed by --param',
            'options': [
                OPT.param,
            ],
        },
        'set': {
            'msg': 'set a node configuration parameter (pointed by --param) value (pointed by --value)',
            'options': [
                OPT.param,
                OPT.value,
            ],
        },
        'unset': {
            'msg': 'unset a node configuration parameter (pointed by --param)',
            'options': [
                OPT.param,
            ],
        },
    },
    'Push data to the collector': {
        'pushasset': {
            'msg': 'push asset information to collector',
            'options': [
                OPT.sync,
            ],
         },
        'pushstats': {
            'msg': 'push performance metrics to collector. By default pushed '
                   'stats interval begins yesterday at the beginning of the '
                   'allowed interval and ends now. This interval can be '
                   'changed using --begin/--end parameters. The location '
                   'where stats files are looked up can be changed using '
                   '--stats-dir.',
            'options': [
                OPT.begin,
                OPT.end,
                OPT.stats_dir,
            ],
         },
        'pushdisks': {
            'msg': 'push disks usage information to collector',
         },
        'pushpkg': {
            'msg': 'push package/version list to collector',
         },
        'pushpatch': {
            'msg': 'push patch/version list to collector',
         },
        'pushsym': {
            'msg': 'push symmetrix configuration to collector',
            'options': [
                OPT.opt_object,
                OPT.symcli_db_file,
            ],
         },
        'pushemcvnx': {
            'msg': 'push EMC CX/VNX configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushcentera': {
            'msg': 'push EMC Centera configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushnetapp': {
            'msg': 'push Netapp configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pusheva': {
            'msg': 'push HP EVA configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushnecism': {
            'msg': 'push NEC ISM configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushhds': {
            'msg': 'push HDS configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushdcs': {
            'msg': 'push Datacore configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushfreenas': {
            'msg': 'push FreeNAS configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushibmsvc': {
            'msg': 'push IBM SVC configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushhp3par': {
            'msg': 'push HP 3par configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushibmds': {
            'msg': 'push IBM DS configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushvioserver': {
            'msg': 'push IBM VIO server configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushgcedisks': {
            'msg': 'push Google Compute Engine disks configuration to '
                   'collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushbrocade': {
            'msg': 'push Brocade switch configuration to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'pushnsr': {
            'msg': 'push EMC Networker index to collector',
            'options': [
                OPT.opt_object,
            ],
         },
        'sysreport': {
            'msg': 'push system report to the collector for archiving and '
                   'diff analysis',
         },
        'checks': {
            'msg': 'run node sanity checks, push results to collector',
         },
    },
    'Misc': {
        'prkey': {
            'msg': 'show persistent reservation key of this node',
         },
    },
    'Compliance': {
        'compliance_auto': {
            'msg': 'run compliance checks or fix, according to the autofix '
                   'property of each module. --ruleset <md5> instruct the '
                   'collector to provide an historical ruleset.',
        },
        'compliance_env': {
            'msg': 'show the compliance modules environment variables.',
            'options': [
                OPT.module,
                OPT.moduleset,
            ],
        },
        'compliance_check': {
            'msg': 'run compliance checks. --ruleset <md5> instruct the '
                   'collector to provide an historical ruleset.',
            'options': [
                OPT.attach,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        'compliance_fix': {
            'msg': 'run compliance fixes. --ruleset <md5> instruct the '
                   'collector to provide an historical ruleset.',
            'options': [
                OPT.attach,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        'compliance_fixable': {
            'msg': 'verify compliance fixes prerequisites. --ruleset <md5> '
                   'instruct the collector to provide an historical ruleset.',
            'options': [
                OPT.attach,
                OPT.module,
                OPT.moduleset,
                OPT.ruleset_date,
            ],
        },
        'compliance_list_module': {
            'msg': 'list compliance modules available on this node',
        },
        'compliance_show_moduleset': {
            'msg': 'show compliance rules applying to this node',
        },
        'compliance_list_moduleset': {
            'msg': 'list available compliance modulesets. --moduleset f% '
                   'limit the scope to modulesets matching the f% pattern.',
        },
        'compliance_attach_moduleset': {
            'msg': 'attach moduleset specified by --moduleset for this node',
            'options': [
                OPT.moduleset,
            ],
        },
        'compliance_detach_moduleset': {
            'msg': 'detach moduleset specified by --moduleset for this node',
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
        'compliance_show_status': {
            'msg': 'show compliance modules status',
        },
        'compliance_attach': {
            'msg': 'attach ruleset specified by --ruleset and/or moduleset '
                   'specified by --moduleset for this node',
            'options': [
                OPT.moduleset,
                OPT.ruleset,
            ],
        },
        'compliance_detach': {
            'msg': 'detach ruleset specified by --ruleset and/or moduleset '
                   'specified by --moduleset for this node',
            'options': [
                OPT.moduleset,
                OPT.ruleset,
            ],
        },
        'compliance_attach_ruleset': {
            'msg': 'attach ruleset specified by --ruleset for this node',
            'options': [
                OPT.ruleset,
            ],
        },
        'compliance_detach_ruleset': {
            'msg': 'detach ruleset specified by --ruleset for this node',
            'options': [
                OPT.ruleset,
            ],
        },
    },
    'Collector management': {
        'collector_cli': {
            'msg': 'open a Command Line Interface to the collector rest API. '
                   'The CLI offers autocompletion of paths and arguments, '
                   'piping JSON data from files. This command accepts the '
                   '--user, --password, --api, --insecure and --config '
                   'parameters. If executed as root, the collector is '
                   'logged in with the node credentials.',
            'options': [
                OPT.user,
                OPT.password,
                OPT.api,
                OPT.insecure,
                OPT.config,
                OPT.refresh_api,
            ],
        },
        'collector_events': {
            'msg': 'display node events during the period specified by '
                   '--begin/--end. --end defaults to now. --begin defaults to '
                   '7 days ago.',
            'options': [
                OPT.begin,
                OPT.end,
            ],
        },
        'collector_alerts': {
            'msg': 'display node alerts',
        },
        'collector_checks': {
            'msg': 'display node checks',
        },
        'collector_disks': {
            'msg': 'display node disks',
        },
        'collector_list_actions': {
            'msg': 'list actions on the node, whatever the service, during '
                   'the period specified by --begin/--end. --end defaults to '
                   'now. --begin defaults to 7 days ago',
            'options': [
                OPT.begin,
                OPT.end,
            ],
        },
        'collector_ack_action': {
            'msg': 'acknowledge an action error on the node. an acknowlegment '
                   'can be completed by --author (defaults to root@nodename) '
                   'and --comment',
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
            ],
        },
        'collector_list_nodes': {
            'msg': 'show the list of nodes matching the filterset pointed by '
                   '--filterset',
        },
        'collector_list_services': {
            'msg': 'show the list of services matching the filterset pointed '
                   'by --filterset',
        },
        'collector_list_filtersets': {
            'msg': 'show the list of filtersets available on the collector. '
                   'if specified, --filterset <pattern> limits the resulset '
                   'to filtersets matching <pattern>',
        },
        'collector_log': {
            'msg': 'log a message in the collector\'s node log',
            'options': [
                OPT.message,
            ],
        },
        'collector_asset': {
            'msg': 'display asset information known to the collector',
        },
        'collector_networks': {
            'msg': 'display network information known to the collector for '
                   'each node ip',
        },
        'collector_tag': {
            'msg': 'set a node tag (pointed by --tag)',
            'options': [
                OPT.tag,
            ],
        },
        'collector_untag': {
            'msg': 'unset a node tag (pointed by --tag)',
        },
        'collector_show_tags': {
            'msg': 'list all node tags',
        },
        'collector_list_tags': {
            'msg': 'list all available tags. use --like to filter the output.',
            'options': [
                OPT.like,
            ],
        },
        'collector_create_tag': {
            'msg': 'create a new tag with name specified by --tag',
            'options': [
                OPT.tag,
            ],
        },
        'collector_search': {
            'msg': 'report the collector objects matching --like '
                   '[<type>:]<substring>, where <type> is the object type '
                   'acronym as shown in the collector search widget.',
            'options': [
                OPT.like,
            ],
        },
    },
}

DEPRECATED_ACTIONS = [
    "collector_json_asset",
    "collector_json_networks",
    "collector_json_list_unavailability_ack",
    "collector_json_list_actions",
    "collector_json_show_actions",
    "collector_json_status",
    "collector_json_checks",
    "collector_json_disks",
    "collector_json_alerts",
    "collector_json_events",
    "collector_json_list_nodes",
    "collector_json_list_services",
    "collector_json_list_filtersets",
    "json_schedule",
]

class NodemgrOptParser(OptParser):
    """
    The nodemgr-specific options parser class
    """
    def __init__(self, args=None, colorize=True, width=None, formatter=None,
                 indent=6):
        OptParser.__init__(self, args=args, prog=PROG, options=OPT,
                           actions=ACTIONS,
                           deprecated_actions=DEPRECATED_ACTIONS,
                           global_options=GLOBAL_OPTS,
                           colorize=colorize, width=width,
                           formatter=formatter, indent=indent)

