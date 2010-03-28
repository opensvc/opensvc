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
	'diskupdate':	'update var files associated to disks',
	'prstart':	'reserve scsi disks held by this service',
	'prstop':	'release scsi disks held by this service',
	'prstatus':	'report status of reservations on scsi disks held by this service',
	'restart':	'stop then start',
	'syncquiesce':	'trigger a storage hardware-assisted disk synchronization',
	'syncbreak':	'split a storage hardware-assisted disk synchronization',
	'syncresync':	're-establish a broken storage hardware-assisted synchronization',
	'syncfullsync':	'trigger a full copy of the volume to its target',
	'syncupdate':	'trigger a one-time resync of the volume to its target',
	'syncall':	'combo action, chaining diskupdate-syncnodes-syncdrp-syncupdate',
	'push':         'push service configuration to database',
}

def format_desc(svc=False):
        from textwrap import TextWrapper
        wrapper = TextWrapper(subsequent_indent="%19s"%"", width=78)
	desc = "Supported commands:\n"
	for a in sorted(action_desc):
		if svc and not hasattr(svc, a):
			continue
		text = "  %-16s %s\n"%(a, action_desc[a])
                desc += wrapper.fill(text)
                desc += '\n'
	return desc

class svcOptionParser:
	def __init__(self, svc=False):
		__ver = rcEnv.prog + " version " + str(rcEnv.ver)
		__usage = "%prog [options] command\n\n" + format_desc(svc)
		parser = optparse.OptionParser(version=__ver, usage=__usage)
		parser.add_option("--debug", default="False",
				  action="store_true", dest="debug",
				  help="debug mode")
		parser.add_option("-f", "--force", default=False,
				  action="store_true", dest="force",
				  help="force action, ignore sanity check warnings")
		(self.options, self.args) = parser.parse_args()
		if len(self.args) > 1:
			parser.error("More than one action")
		if len(self.args) is 0:
			parser.error("Missing action")
		self.action = self.args[0]
		if svc and not hasattr(svc, self.action):
			parser.error("unsupported action")
                if svc:
			svc.force = self.options.force
