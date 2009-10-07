import os, sys
import select
import logging
from subprocess import *

def process_call_argv(argv):
	log = logging.getLogger('CALL')
	if not argv:
		return (0, '')
	log.debug(' '.join(argv))
	process = Popen(argv, stdout=PIPE, close_fds=True)
	output = process.communicate()[0]
	return (process.returncode, output)
