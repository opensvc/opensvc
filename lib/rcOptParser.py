#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import optparse
from rcGlobalEnv import *

action_desc = {
	'printsvc':	'display service live configuration',
	'boot':	        'start a service if executed on the primary node (or one of the primary nodes in case of a flex service), startstandby if not',
	'shutdown':     'stop a service, disabling the background database logging',
	'start':	'start a service, chaining startip-diskstart-startapp',
	'startstandby':	'start service resources marked always_on',
	'startip':	'configure service ip addresses',
	'mount':	'prepare devices, logical volumes, mount service filesystems, bootstrap containers',
	'startapp':	'execute service application startup script',
	'stop':		'stop a service, chaining stopapp-diskstop-stopip',
	'stopip':	'unconfigure service ip addresses',
	'umount':	'shutdown container, umount service filesystems, deactivate logical volumes',
	'stopapp':	'execute service application stop script',
	'syncnodes':	'send to peer nodes the service config files and additional files described in the config file',
	'syncdrp':	'send to drp nodes the service config files and additional files described in the config file',
	'configure':	'configure a service container, using configuration file information',
	'create':	'create a new service configuration file',
	'update':	'update definitions in an existing service configuration file',
	'disable':	'disable resources passed through --rid in services passed through --service',
	'enable':	'enable resources passed through --rid in services passed through --service',
	'delete':	'delete resources passed through --rid in services passed through --service',
	'startcontainer':	'start the container resource',
	'stopcontainer':	'stop the container resource',
	'status':	'return service overall status code',
	'print_status':	'display service resource status',
	'freeze':	'set up a flag to block actions on this service',
	'thaw':		'remove the flag to unblock actions on this service',
	'frozen':	'report on the current blocking of actions on this service',
	'startloop':	'attach loop resources of this service',
	'stoploop':	'detach loop resources of this service',
	'startvg':	'activate/import volume group resources of this service',
	'stopvg':	'deactivate/deport volume group resources of this service',
	'startpool':	'activate zpool resources of this service',
	'stoppool':	'deactivate zpool resources of this service',
	'startdisk':	'combo action, chaining startvg-mount',
	'stopdisk':	'combo action, chaining umount-stopvg',
	'presync':	'update var files associated to resources',
	'postsync':	'make use of files received from master nodes in var',
	'prstart':	'reserve scsi disks held by this service',
	'prstop':	'release scsi disks held by this service',
	'prstatus':	'report status of reservations on scsi disks held by this service',
	'restart':	'combo action, chaining stop-start',
	'resync':	'combo action, chaining stop-syncresync-start',
	'syncquiesce':	'trigger a storage hardware-assisted disk synchronization',
	'syncbreak':	'split a storage hardware-assisted disk synchronization',
	'syncresync':	're-establish a broken storage hardware-assisted synchronization',
	'syncfullsync':	'trigger a full copy of the volume to its target',
	'syncupdate':	'trigger a one-time resync of the volume to its target',
	'syncverify':	'trigger a one-time checksum-based verify of the volume and its target',
	'syncall':	'combo action, chaining syncnodes-syncdrp-syncupdate',
	'push':         'push service configuration to database',
        'disklist':     'construct disklist',
        'switch':       'stop the service on the local node and start on the remote node',
        'migrate':      'live migrate the service to the remote node',
        'json_status':  'provide the resource and aggregated status in json format, for use by tier tools',
        'resource_monitor':  'detect monitored resource failures and trigger monitor_action',
        'stonith':      'command provided to the heartbeat daemon to fence peer node in case of split brain',
        'set':      'set a service configuration parameter (pointed by --param) value (pointed by --value)',
        'get':      'get the value of the node configuration parameter pointed by --param',
        'unset':      'unset a node configuration parameter pointed by --param',
        'compliance_check': 'run compliance checks',
        'compliance_fix':   'run compliance fixes',
        'compliance_fixable': 'verify compliance fixes prerequisites',
        'compliance_show_moduleset': 'show compliance rules applying to this node',
        'compliance_list_moduleset': 'list available compliance modulesets. --moduleset f% limit the scope to modulesets matching the f% pattern.',
        'compliance_attach_moduleset': 'attach moduleset specified by --moduleset for this node',
        'compliance_detach_moduleset': 'detach moduleset specified by --moduleset for this node',
        'compliance_list_ruleset': 'list available compliance rulesets. --ruleset f% limit the scope to rulesets matching the f% pattern.',
        'compliance_show_ruleset': 'show compliance rules applying to this node',
        'compliance_attach_ruleset': 'attach ruleset specified by --ruleset for this node',
        'compliance_detach_ruleset': 'detach ruleset specified by --ruleset for this node',
        'collector_ack_unavailability': 'acknowledge an unavailability period. the period is specified by --begin/--end or --begin/--duration. omitting --begin defaults to now. an acknowlegment can be completed by --author (defaults to root@nodename), --account (default to 1) and --comment',
        'collector_list_unavailability_ack': 'list acknowledged periods for the service. the periods can be filtered by --begin/--end. omitting --end defaults to now. the wildcartd for --comment and --author is %',
        'collector_list_actions': 'list actions on the service, whatever the node, during the period specified by --begin/--end. --end defaults to now. --begin defaults to 7 days ago',
        'collector_ack_action': 'acknowledge an action error on the service. an acknowlegment can be completed by --author (defaults to root@nodename) and --comment',
        'collector_show_actions': 'show actions detailled log. a single action is specified by --id. a range is specified by --begin/--end dates. --end defaults to now. --begin defaults to 7 days ago',
        'collector_status': 'display service status on all nodes, according to the collector.',
        'collector_checks': 'display service checks',
        'collector_alerts': 'display service alerts',
}

def format_desc(svc=False):
    from textwrap import TextWrapper
    wrapper = TextWrapper(subsequent_indent="%19s"%"", width=78)
    desc = "Supported commands:\n"
    for a in sorted(action_desc):
        if svc and not hasattr(svc, a):
            continue
        fancya = a.replace('_', ' ')
        if len(a) < 16:
            text = "  %-16s %s\n"%(fancya, action_desc[a])
            desc += wrapper.fill(text)
        else:
            text = "  %-16s"%(fancya)
            desc += wrapper.fill(text)
            desc += '\n'
            text = "%19s%s"%(" ", action_desc[a])
            desc += wrapper.fill(text)
        desc += '\n\n'
    return desc

