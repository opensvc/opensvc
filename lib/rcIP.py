import socket

class ip:
	def __init__(self, name, dev):
		self.name = name
		self.dev = dev
		self.addr = socket.gethostbyname(name)
