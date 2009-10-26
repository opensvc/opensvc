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
	'start':	'start a service, chaining startip-diskstart-startapp',
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
	'startlxc':	'start a LinuX Container',
	'stoplxc':	'stop a LinuX Container',
	'status':	'display service resource status',
	'freeze':	'set up a flag to block actions on this service',
	'thaw':		'remove the flag to unblock actions on this service',
	'frozen':	'report on the current blocking of actions on this service',
	'startloop':	'attach loop resources of this service',
	'stoploop':	'detach loop resources of this service',
	'startvg':	'activate/import volume group resources of this service',
	'stopvg':	'deactivate/deport volume group resources of this service',
	'diskstart':	'combo action, chaining startvg-mount',
	'diskstop':	'combo action, chaining umount-stopvg',
	'scsireserv':	'reserve scsi disks held by this service',
	'scsirelease':	'release scsi disks held by this service',
}

def format_desc(svc=False):
	desc = "Commands available to this service:\n"
	for a in action_desc.keys():
		if svc and not hasattr(svc, a):
			continue
		desc += "  {0:13s}{1:65s}\n".format(a, action_desc[a])
	return desc

class svcOptionParser:
	def __init__(self, svc=False):
		__ver = rcEnv.prog + " version " + str(rcEnv.ver)
		__usage = "%prog [options] command\n\n" + format_desc(svc)
		parser = optparse.OptionParser(version=__ver, usage=__usage)
		parser.add_option("--debug", default="False",
				  action="store_true", dest="debug",
				  help="debug mode")
		parser.add_option("-f", "--force", default="False",
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
	def show(self):
		print ":: option parser dump"
		print "{0:15s}{1:15s}".format('debug', str(self.options.debug))
		print "{0:15s}{1:15s}".format('force', str(self.options.force))
		print "{0:15s}{1:15s}".format('action', self.action)
