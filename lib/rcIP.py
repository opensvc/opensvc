import socket

class ip:
	def __init__(self, name, dev, netmask):
		self.name = name
		self.dev = dev
		self.netmask = netmask
		self.addr = socket.gethostbyname(name)
