import logging
import logging.handlers
from rcGlobalEnv import *

class Logger(logging.Logger):
	def __init__(self, name):
		logging.Logger.__init__(self, name)
		formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
		filehandler = logging.handlers.RotatingFileHandler(rcEnv.logfile, maxBytes=5000, backupCount=5)
		streamhandler = logging.StreamHandler()
		filehandler.setFormatter(formatter)
		streamhandler.setFormatter(formatter)
		self.addHandler(filehandler)
		self.addHandler(streamhandler)
		try:
			self.setLevel(rcEnv.loglevel)
		except:
			pass
