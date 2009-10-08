import os, sys
import select
import logging
from subprocess import *

def banner(text, ch='=', length=78):
	spaced_text = ' %s ' % text
	banner = spaced_text.center(length, ch)
	return banner

def is_exe(fpath):
	"""Returns True if file path is executable, False otherwize
	"""
	return os.path.exists(fpath) and os.access(fpath, os.X_OK)

def which(program):
	"""Returns True if program is in PATH and executable, False
	otherwize
	"""
	fpath, fname = os.path.split(program)
	if fpath and is_exe(program):
		return program
	for path in os.environ["PATH"].split(os.pathsep):
		exe_file = os.path.join(path, program)
		if is_exe(exe_file):
			return exe_file
	return None

def process_call_argv(argv):
	log = logging.getLogger('CALL')
	if not argv:
		return (0, '')
	log.debug(' '.join(argv))
	process = Popen(argv, stdout=PIPE, close_fds=True)
	output = process.communicate()[0]
	return (process.returncode, output)
