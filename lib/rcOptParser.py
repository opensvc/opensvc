"""
Helper module to handle svcmgr optparser configuration.

Define a reference of supported keywords, their supported options, and methods
to format contextualized svcmgr help messages.
"""

ACTION_DESC = {
    'Service actions': {
        'boot': {
            'msg': 'start a service if executed on the primary node (or one of'
                   ' the primary nodes in case of a flex service), '
                   'startstandby if not',
        },
        'shutdown': {
            'msg': 'stop a service, disabling the background database logging',
        },
        'start': {
            'msg': 'start all service resources',
        },
        'startstandby': {
            'msg': 'start service resources flagged always on',
        },
        'startip': {
            'msg': 'configure service ip addresses',
        },
        'startshare': {
            'msg': 'start network shares',
        },
        'stopshare': {
            'msg': 'stop network shares',
        },
        'startfs': {
            'msg': 'prepare devices, logical volumes, mount service '
                   'filesystems, bootstrap containers',
        },
        'startapp': {
            'msg': 'execute service application startup script',
        },
        'stop': {
            'msg': 'stop all service resources not flagged always on. With '
                   '--force, stop all service resources, even those flagged '
                   'always on.',
        },
        'stopip': {
            'msg': 'unconfigure service ip addresses',
        },
        'stopfs': {
            'msg': 'shutdown container, umount service filesystems, deactivate'
                   ' logical volumes',
        },
        'stopapp': {
            'msg': 'execute service application stop script',
        },
        'startcontainer': {
            'msg': 'start the container resource',
        },
        'stopcontainer': {
            'msg': 'stop the container resource',
        },
        'provision': {
            'msg': 'provision and start the service',
        },
        'unprovision': {
            'msg': 'stop and unprovision the service. beware: data will be '
                   'lost upon fs and disk unprovisioning.',
        },
        'disable': {
            'msg': 'disable resources passed through --rid in services passed'
                   ' through --service. Specifying no resource disables the '
                   'whole service.',
        },
        'enable': {
            'msg': 'enable resources passed through --rid in services passed'
                   ' through --service. Specifying no resource enables the '
                   'whole service.',
        },
        'status': {
            'msg': 'return service overall status code',
        },
        'print_status': {
            'msg': 'display service resource status',
        },
        'print_resource_status': {
            'msg': 'display a specific service resource status, pointed by'
                   ' --rid',
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
        },
        'startloop': {
            'msg': 'attach loop resources of this service',
        },
        'stoploop': {
            'msg': 'detach loop resources of this service',
        },
        'startvg': {
            'msg': 'activate/import volume group resources of this service',
        },
        'stopvg': {
            'msg': 'deactivate/deport volume group resources of this service',
        },
        'startpool': {
            'msg': 'activate zpool resources of this service',
        },
        'stoppool': {
            'msg': 'deactivate zpool resources of this service',
        },
        'startdisk': {
            'msg': 'combo action, activating standby disks, taking '
                   'reservations, starting loopback devices and volume '
                   'groups',
        },
        'stopdisk': {
            'msg': 'combo action, stopping volume groups and loopback '
                   'devices, droping reservations, disabling standby disks',
        },
        'presync': {
            'msg': 'update var files associated to resources',
        },
        'postsync': {
            'msg': 'make use of files received from master nodes in var',
        },
        'prstart': {
            'msg': 'reserve scsi disks held by this service',
        },
        'prstop': {
            'msg': 'release scsi disks held by this service',
        },
        'prstatus': {
            'msg': 'report status of reservations on scsi disks held by this '
                   'service',
        },
        'restart': {
            'msg': 'combo action, chaining stop-start',
        },
        'resync': {
            'msg': 'combo action, chaining stop-sync_resync-start',
        },
        'sync_nodes': {
            'msg': 'send to peer nodes the service config files and '
                   'additional files described in the config file.',
        },
        'sync_drp': {
            'msg': 'send to drp nodes the service config files and '
                   'additional files described in the config file.',
        },
        'sync_quiesce': {
            'msg': 'trigger a storage hardware-assisted disk synchronization',
        },
        'sync_break': {
            'msg': 'split a storage hardware-assisted disk synchronization',
        },
        'sync_split': {
            'msg': 'split a EMC SRDF storage hardware-assisted disk '
                   'synchronization',
        },
        'sync_establish': {
            'msg': 'establish a EMC SRDF storage hardware-assisted disk '
                   'synchronization',
        },
        'sync_resync': {
            'msg': 'like sync_update, but not triggered by the scheduler '
                   '(thus adapted for clone/snap operations)',
        },
        'sync_full': {
            'msg': 'trigger a full copy of the volume to its target',
        },
        'sync_restore': {
            'msg': 'trigger a restore of the sync resources data to their '
                   'target path (DANGEROUS: make sure you understand before '
                   'running this action).',
        },
        'sync_update': {
            'msg': 'trigger a one-time resync of the volume to its target',
        },
        'sync_resume': {
            'msg': 're-establish a broken storage hardware-assisted '
                   'synchronization',
        },
        'sync_revert': {
            'msg': 'revert to the pre-failover data (looses current data)',
        },
        'sync_verify': {
            'msg': 'trigger a one-time checksum-based verify of the volume '
                   'and its target',
        },
        'sync_all': {
            'msg': 'combo action, chaining sync_nodes-sync_drp-sync_update.',
        },
        'push': {
            'msg': 'push service configuration to the collector',
        },
        'pull': {
            'msg': 'pull a service configuration from the collector',
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
        },
        'print_devlist': {
            'msg': 'print service device list',
        },
        'switch': {
            'msg': 'stop the service on the local node and start on the '
                   'remote node. --to <node> specify the remote node to '
                   'switch the service to.',
        },
        'migrate': {
            'msg': 'live migrate the service to the remote node. '
                   '--to <node> specify the remote node to migrate the '
                   'service to.',
        },
        'resource_monitor': {
            'msg': 'detect monitored resource failures and trigger '
                   'monitor_action',
        },
        'stonith': {
            'msg': 'command provided to the heartbeat daemon to fence peer '
                   'node in case of split brain',
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
        },
        'scheduler': {
            'msg': 'run the service task scheduler',
        },
        'pg_freeze': {
            'msg': 'freeze the tasks of a process group',
        },
        'pg_thaw': {
            'msg': 'thaw the tasks of a process group',
        },
        'pg_kill': {
            'msg': 'kill the tasks of a process group',
        },
        'logs': {
            'msg': 'display the service logs in the pager',
        },
    },
    'Service configuration': {
        'print_config': {
            'msg': 'display service current configuration',
        },
        'edit_config': {
            'msg': 'edit service configuration',
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
        },
        'update': {
            'msg': 'update definitions in an existing service configuration '
                   'file',
        },
        'delete': {
            'msg': 'delete the service instance on the local node if no '
                   '--rid is specified, or delete the resources pointed by '
                   '--rid in services passed through --service',
        },
        'set': {
            'msg': 'set a service configuration parameter (pointed by '
                   '--param) value (pointed by --value)',
        },
        'get': {
            'msg': 'get the value of the node configuration parameter '
                   'pointed by --param',
        },
        'unset': {
            'msg': 'unset a node configuration parameter pointed by --param',
        },
    },
    'Compliance': {
        'compliance_check': {
            'msg': 'run compliance checks. --ruleset <md5> instruct the '
                   'collector to provide an historical ruleset.',
        },
        'compliance_env': {
            'msg': 'show the compliance modules environment variables.',
        },
        'compliance_fix': {
            'msg': 'run compliance fixes. --ruleset <md5> instruct the '
                   'collector to provide an historical ruleset.',
        },
        'compliance_fixable': {
            'msg': 'verify compliance fixes prerequisites. --ruleset <md5>'
                   ' instruct the collector to provide an historical ruleset.',
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
        },
        'compliance_detach_moduleset': {
            'msg': 'detach moduleset specified by --moduleset from this '
                   'service',
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
        },
        'compliance_detach_ruleset': {
            'msg': 'detach ruleset specified by --ruleset from this service',
        },
        'compliance_attach': {
            'msg': 'attach ruleset specified by --ruleset and/or moduleset '
                   'specified by --moduleset to this service',
        },
        'compliance_detach': {
            'msg': 'detach ruleset specified by --ruleset and/or moduleset '
                   'specified by --moduleset from this service',
        },
    },
    'Collector management': {
        'collector_ack_unavailability': {
            'msg': 'acknowledge an unavailability period. the period is '
                   'specified by --begin/--end or --begin/--duration. '
                   'omitting --begin defaults to now. an acknowlegment can '
                   'be completed by --author (defaults to root@nodename), '
                   '--account (default to 1) and --comment',
        },
        'collector_list_unavailability_ack': {
            'msg': 'list acknowledged periods for the service. the periods '
                   'can be filtered by --begin/--end. omitting --end '
                   'defaults to now. the wildcartd for --comment and '
                   '--author is %',
        },
        'collector_list_actions': {
            'msg': 'list actions on the service, whatever the node, during '
                   'the period specified by --begin/--end. --end defaults to '
                   'now. --begin defaults to 7 days ago',
        },
        'collector_ack_action': {
            'msg': 'acknowledge an action error on the service. an '
                   'acknowlegment can be completed by --author (defaults '
                   'to root@nodename) and --comment',
        },
        'collector_show_actions': {
            'msg': 'show actions detailed log. a single action is specified '
                   'by --id. a range is specified by --begin/--end dates. '
                   '--end defaults to now. --begin defaults to 7 days ago',
        },
        'collector_status': {
            'msg': 'display service status on all nodes, according to the '
                   'collector.',
        },
        'collector_checks': {
            'msg': 'display service checks',
        },
        'collector_disks': {
            'msg': 'display service disks',
        },
        'collector_alerts': {
            'msg': 'display service alerts',
        },
        'collector_events': {
            'msg': 'display service events during the period specified by '
                   '--begin/--end. --end defaults to now. --begin defaults '
                   'to 7 days ago',
        },
        'collector_asset': {
            'msg': 'display asset information known to the collector',
        },
        'collector_networks': {
            'msg': 'display network information known to the collector for '
                   'each service ip',
        },
        'collector_tag': {
            'msg': 'set a service tag (pointed by --tag)',
        },
        'collector_untag': {
            'msg': 'unset a service tag (pointed by --tag)',
        },
        'collector_show_tags': {
            'msg': 'list all service tags',
        },
        'collector_list_tags': {
            'msg': 'list all available tags. use --like to filter the output.',
        },
        'collector_create_tag': {
            'msg': 'create a new tag',
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

def format_desc(svc=False, action=None):
    """
    Format and return a svcmgr parser help message, contextualized to display
    only actions matching the action argument.
    """
    from textwrap import TextWrapper
    wrapper = TextWrapper(subsequent_indent="%19s"%"", width=78)
    desc = ""
    for section in sorted(ACTION_DESC):
        valid_actions = []
        for candidate_action in sorted(ACTION_DESC[section]):
            if isinstance(action, str) and \
               not candidate_action.startswith(action):
                continue
            if isinstance(action, list) and candidate_action not in action:
                continue
            valid_actions.append(candidate_action)
        if len(valid_actions) == 0:
            continue

        desc += section + '\n'
        desc += '-' * len(section)
        desc += "\n\n"
        for valid_action in valid_actions:
            if svc and not hasattr(svc, valid_action):
                continue
            fancya = "svcmgr " + valid_action.replace('_', ' ')
            if len(fancya) < 15:
                text = "  %-16s %s\n" % (fancya, ACTION_DESC[section][valid_action]["msg"])
                desc += wrapper.fill(text)
            else:
                text = "  %-16s"%(fancya)
                desc += wrapper.fill(text)
                desc += '\n'
                text = "%19s%s" % (" ", ACTION_DESC[section][valid_action]["msg"])
                desc += wrapper.fill(text)
            desc += '\n\n'
    return desc[0:-2]

def supported_actions():
    """
    Return the list of actions supported by svcmgr.
    """
    actions = []
    for section in ACTION_DESC:
        actions += ACTION_DESC[section].keys()
    actions += DEPRECATED_ACTIONS
    return actions
