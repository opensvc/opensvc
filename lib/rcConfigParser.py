import ConfigParser

def load_cf(cf):
	with open(cf) as f:
		for line in f:
			print line 

class RawConfigParser(ConfigParser.RawConfigParser):
	def __init__(self, capabilities):
		print "THERE"
