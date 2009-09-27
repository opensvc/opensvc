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


