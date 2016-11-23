import optparse
from rcGlobalEnv import *

action_desc = {
    'Service actions': {
      'boot': 'start a service if executed on the primary node (or one of the primary nodes in case of a flex service), startstandby if not',
      'shutdown': 'stop a service, disabling the background database logging',
      'start': 'start all service resources',
      'startstandby': 'start service resources flagged always on',
      'startip': 'configure service ip addresses',
      'startshare': 'start network shares',
      'stopshare': 'stop network shares',
      'startfs': 'prepare devices, logical volumes, mount service filesystems, bootstrap containers',
      'startapp': 'execute service application startup script',
      'stop': 'stop all service resources not flagged always on. With --force, stop all service resources, even those flagged always on.',
      'stopip': 'unconfigure service ip addresses',
      'stopfs': 'shutdown container, umount service filesystems, deactivate logical volumes',
      'stopapp': 'execute service application stop script',
      'startcontainer': 'start the container resource',
      'stopcontainer': 'stop the container resource',
      'provision': 'provision and start the service',
      'unprovision': 'stop and unprovision the service. beware: data will be lost upon fs and disk unprovisioning.',
      'disable': 'disable resources passed through --rid in services passed through --service. Specifying no resource disables the whole service.',
      'enable': 'enable resources passed through --rid in services passed through --service. Specifying no resource enables the whole service.',
      'status': 'return service overall status code',
      'print_status': 'display service resource status',
      'print_resource_status': 'display a specific service resource status, pointed by --rid',
      'print_config_mtime': 'display service configuration file modification time',
      'freeze': 'set up a flag to block actions on this service',
      'thaw': 'remove the flag to unblock actions on this service',
      'frozen': 'report on the current blocking of actions on this service',
      'run': 'run all tasks, or tasks specified by --rid --tags and --subset, disregarding their schedule',
      'startloop': 'attach loop resources of this service',
      'stoploop': 'detach loop resources of this service',
      'startvg': 'activate/import volume group resources of this service',
      'stopvg': 'deactivate/deport volume group resources of this service',
      'startpool': 'activate zpool resources of this service',
      'stoppool': 'deactivate zpool resources of this service',
      'startdisk': 'combo action, activating standby disks, taking reservations, starting loopback devices and volume groups',
      'stopdisk': 'combo action, stopping volume groups and loopback devices, droping reservations, disabling standby disks',
      'presync': 'update var files associated to resources',
      'postsync': 'make use of files received from master nodes in var',
      'prstart': 'reserve scsi disks held by this service',
      'prstop': 'release scsi disks held by this service',
      'prstatus': 'report status of reservations on scsi disks held by this service',
      'restart': 'combo action, chaining stop-start',
      'resync': 'combo action, chaining stop-sync_resync-start',
      'sync_nodes': 'send to peer nodes the service config files and additional files described in the config file.',
      'sync_drp': 'send to drp nodes the service config files and additional files described in the config file.',
      'sync_quiesce': 'trigger a storage hardware-assisted disk synchronization',
      'sync_break': 'split a storage hardware-assisted disk synchronization',
      'sync_split': 'split a EMC SRDF storage hardware-assisted disk synchronization',
      'sync_establish': 'establish a EMC SRDF storage hardware-assisted disk synchronization',
      'sync_resync': 'like sync_update, but not triggered by the scheduler (thus adapted for clone/snap operations)',
      'sync_full': 'trigger a full copy of the volume to its target',
      'sync_restore': 'trigger a restore of the sync resources data to their target path (DANGEROUS: make sure you understand before running this action).',
      'sync_update': 'trigger a one-time resync of the volume to its target',
      'sync_resume': 're-establish a broken storage hardware-assisted synchronization',
      'sync_revert': 'revert to the pre-failover data (looses current data)',
      'sync_verify': 'trigger a one-time checksum-based verify of the volume and its target',
      'sync_all': 'combo action, chaining sync_nodes-sync_drp-sync_update.',
      'push': 'push service configuration to the collector',
      'pull': 'pull a service configuration from the collector',
      'push_resinfo': 'push service resources and application launchers info key/value pairs the collector',
      'push_service_status': 'push service and its resources status to database',
      'print_disklist': 'print service disk list',
      'print_devlist': 'print service device list',
      'switch': 'stop the service on the local node and start on the remote node. --to <node> specify the remote node to switch the service to.',
      'migrate': 'live migrate the service to the remote node. --to <node> specify the remote node to migrate the service to.',
      'resource_monitor': 'detect monitored resource failures and trigger monitor_action',
      'stonith': 'command provided to the heartbeat daemon to fence peer node in case of split brain',
      'docker': 'wrap the docker client command, setting automatically the socket parameter to join the service-private docker daemon. The %as_service%, %images% and %instances% words in the wrapped command are replaced by, respectively, the registry login username/password/email parameters to log as a service using <svcname>@<nodename> as the username and the node uuid as password (which is what is expected when the opensvc collector is used as the JWT manager for the registry), the set of docker instance names and images for container resources passing the --tags, --rid and --subsets filters. This is useful to remove all instances of a service or all instances of resources with a tag like "frontend". Note the opensvc filters must be positioned before the docker command in the arguments list.',
      'print_schedule': "print the service tasks schedule",
      'scheduler': "run the service task scheduler",
      'pg_freeze': "freeze the tasks of a process group",
      'pg_thaw': "thaw the tasks of a process group",
      'pg_kill': "kill the tasks of a process group",
      'logs': "display the service logs in the pager",
     },
    'Service configuration': {
      'print_config': 'display service current configuration',
      'edit_config': 'edit service configuration',
      'validate_config': 'check the sections and parameters are valid.',
      'create': 'create a new service configuration file. --interactive triggers the interactive mode. --template <template name>|<template id>|<uri>|<local path> fetchs and installs a service config template. --config <uri>|<local path> fetchs and installs a service config file. --provision create the system resources defined in the service config.',
      'update': 'update definitions in an existing service configuration file',
      'delete': 'delete the service instance on the local node if no --rid is specified, or delete the resources pointed by --rid in services passed through --service',
      'set': 'set a service configuration parameter (pointed by --param) value (pointed by --value)',
      'get': 'get the value of the node configuration parameter pointed by --param',
      'unset': 'unset a node configuration parameter pointed by --param',
     },
    'Compliance': {
      'compliance_check': 'run compliance checks. --ruleset <md5> instruct the collector to provide an historical ruleset.',
      'compliance_env': 'show the compliance modules environment variables.',
      'compliance_fix': 'run compliance fixes. --ruleset <md5> instruct the collector to provide an historical ruleset.',
      'compliance_fixable': 'verify compliance fixes prerequisites. --ruleset <md5> instruct the collector to provide an historical ruleset.',
      'compliance_show_status': 'show compliance modules status',
      'compliance_show_moduleset': 'show compliance rules applying to this service',
      'compliance_list_moduleset': 'list available compliance modulesets. --moduleset f% limit the scope to modulesets matching the f% pattern.',
      'compliance_attach_moduleset': 'attach moduleset specified by --moduleset to this service',
      'compliance_detach_moduleset': 'detach moduleset specified by --moduleset from this service',
      'compliance_list_ruleset': 'list available compliance rulesets. --ruleset f% limit the scope to rulesets matching the f% pattern.',
      'compliance_show_ruleset': 'show compliance rules applying to this node',
      'compliance_attach_ruleset': 'attach ruleset specified by --ruleset to this service',
      'compliance_detach_ruleset': 'detach ruleset specified by --ruleset from this service',
      'compliance_attach': 'attach ruleset specified by --ruleset and/or moduleset specified by --moduleset to this service',
      'compliance_detach': 'detach ruleset specified by --ruleset and/or moduleset specified by --moduleset from this service',
     },
    'Collector management': {
      'collector_ack_unavailability': 'acknowledge an unavailability period. the period is specified by --begin/--end or --begin/--duration. omitting --begin defaults to now. an acknowlegment can be completed by --author (defaults to root@nodename), --account (default to 1) and --comment',
      'collector_list_unavailability_ack': 'list acknowledged periods for the service. the periods can be filtered by --begin/--end. omitting --end defaults to now. the wildcartd for --comment and --author is %',
      'collector_list_actions': 'list actions on the service, whatever the node, during the period specified by --begin/--end. --end defaults to now. --begin defaults to 7 days ago',
      'collector_ack_action': 'acknowledge an action error on the service. an acknowlegment can be completed by --author (defaults to root@nodename) and --comment',
      'collector_show_actions': 'show actions detailled log. a single action is specified by --id. a range is specified by --begin/--end dates. --end defaults to now. --begin defaults to 7 days ago',
      'collector_status': 'display service status on all nodes, according to the collector.',
      'collector_checks': 'display service checks',
      'collector_disks': 'display service disks',
      'collector_alerts': 'display service alerts',
      'collector_events': 'display service events during the period specified by --begin/--end. --end defaults to now. --begin defaults to 7 days ago',
      'collector_asset': 'display asset information known to the collector',
      'collector_networks': 'display network information known to the collector for each service ip',
      'collector_tag': 'set a service tag (pointed by --tag)',
      'collector_untag': 'unset a service tag (pointed by --tag)',
      'collector_show_tags': 'list all service tags',
      'collector_list_tags': 'list all available tags. use --like to filter the output.',
      'collector_create_tag': 'create a new tag',
    },
}

deprecated_actions = [
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
    from textwrap import TextWrapper
    wrapper = TextWrapper(subsequent_indent="%19s"%"", width=78)
    desc = ""
    for s in sorted(action_desc):
        valid_actions = []
        for a in sorted(action_desc[s]):
            if type(action) == str and not a.startswith(action):
                continue
            if type(action) == list and a not in action:
                continue
            valid_actions.append(a)
        if len(valid_actions) == 0:
            continue

        l = len(s)
        desc += s+'\n'
        for i in range(0, l):
            desc += '-'
        desc += "\n\n"
        for a in valid_actions:
            if svc and not hasattr(svc, a):
                continue
            fancya = "svcmgr " + a.replace('_', ' ')
            if len(fancya) < 15:
                text = "  %-16s %s\n"%(fancya, action_desc[s][a])
                desc += wrapper.fill(text)
            else:
                text = "  %-16s"%(fancya)
                desc += wrapper.fill(text)
                desc += '\n'
                text = "%19s%s"%(" ", action_desc[s][a])
                desc += wrapper.fill(text)
            desc += '\n\n'
    return desc[0:-2]

def supported_actions():
    a = []
    for s in action_desc:
        a += action_desc[s].keys()
    a += deprecated_actions
    return a
