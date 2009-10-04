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
class rcEnv:
	"""Class to store globals
	"""

	prog = "opensvc"
	ver = "20090924-1"

	#
	# EZ-HA defines
	# EZ-HA does heartbeat, stonith, automatic service failover
	#
	ez_path = "/usr/local/cluster"
	ez_path_services = ez_path + "/conf/services"

	#
	# True: check_up_script_gen.sh will try a ping + RSH other node before stonith
	#
	ez_last_chance = True

	#
	# True: startapp in background if EZ-HA take-over is succesful
	#
	ez_startapp_bg = True

