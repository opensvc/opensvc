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
import os
import sys
import re
from rcGlobalEnv import *

def node_cap_ez_ha():
	if not os.path.exists(rcEnv.ez_path):
		return False
	if not os.path.exists(rcEnv.ez_path_services):
		return False
	return True

def node_cap_lxc():
	if not os.path.exists("/proc/1/cgroup"):
		return False
	with open("/proc/1/cgroup") as f:
		for line in f:
			if not re.search("devices",line):
				return False
			if not re.search("memory",line):
				return False
			if not re.search("cpuset",line):
				return False
			if not re.search("ns",line):
				return False
	return True

def node_get_pathsvc():
	return os.path.realpath(os.path.dirname(__file__) + "/..")

def node_get_hostmode(d):
	__f = d + "/host_mode"
	if os.path.exists(__f):
		with open(__f) as f:
			for line in f:
				if line is "DEV" or "EXP":
					return line
	print "Set DEV or EXP in " + __f
	sys.exit(1)

def discover_node():
	"""Fill rcEnv class with information from node discovery
	"""
	#
	# paths
	#
	rcEnv.pathsvc = node_get_pathsvc()
	rcEnv.pathbin = rcEnv.pathsvc + "/bin"
	rcEnv.pathetc = rcEnv.pathsvc + "/etc"
	rcEnv.pathlib = rcEnv.pathsvc + "/lib"
	rcEnv.pathlog = rcEnv.pathsvc + "/log"
	rcEnv.pathtmp = rcEnv.pathsvc + "/tmp"
	rcEnv.pathvar = rcEnv.pathsvc + "/var"
	rcEnv.logfile = rcEnv.pathlog + '/' + rcEnv.svcname + '.log'
	rcEnv.svcconf = rcEnv.pathetc + "/" + rcEnv.svcname + ".env"
	rcEnv.svcinitd = rcEnv.pathetc + "/" + rcEnv.svcname + ".d"

	#
	# node desc
	#
	rcEnv.sysname, rcEnv.nodename, x, x, rcEnv.machine = os.uname()
	rcEnv.hostmode = node_get_hostmode(rcEnv.pathvar)

	#
	# node capabilities
	#
	rcEnv.capabilities = []
	if node_cap_lxc():
		rcEnv.capabilities.append("lxc")
	if node_cap_ez_ha():
		rcEnv.capabilities.append("ez_ha")

