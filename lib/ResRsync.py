import os
import logging

from rcGlobalEnv import *
from rcUtilities import process_call_argv, which
import rcStatus

def sync(self, section, log):
	if not self.svc.conf.has_option("default", section):
		log.info('no %s setup for synchronization' % section)
		return 0
	if section not in self.target:
		log.debug('%s => %s sync not applicable to %s', (self.src, self.dst, section))
		return 0
	for node in self.svc.conf.get("default", section).split(' '):
		if node == rcEnv.nodename:
			continue
		dst = node + ':' + self.dst
		cmd = self.cmd
		cmd.append(dst)
		log.info(' '.join(cmd))
		(ret, out) = process_call_argv(cmd)
		if ret != 0:
			log.error("node %s synchronization failed (%s => %s)" % (node, self.src, self.dst))
			return 1
	return 0

class Rsync:
	timeout = 3600
	options = [ '-HpogDtrlvx', '--stats', '--delete', '--force' ]

	def syncnodes(self):
		log = logging.getLogger('SYNCNODES')
		return sync(self, "nodes", log)

	def syncdrp(self):
		log = logging.getLogger('SYNCDRP')
		return sync(self, "drpnode", log)

	def __init__(self, svc, src, dst, exclude='', target=['nodes', 'drpnode']):
		self.svc = svc
		self.src = src
		self.dst = dst
		self.exclude = exclude
		self.target = target
		self.options.append('--timeout=' + str(self.timeout))
		self.cmd = ['rsync'] + self.options + [self.exclude, self.src]
