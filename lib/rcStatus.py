UP = 0
DOWN = 1
WARN = 2
NA = 3
TODO = 4

def _merge(s1, s2):
	"""Merge too status: WARN and TODO taint UP and DOWN
	"""
	if (s1, s2) == (UP, UP): return UP
	if (s1, s2) == (UP, DOWN): return WARN
	if (s1, s2) == (UP, WARN): return WARN
	if (s1, s2) == (UP, NA): return UP
	if (s1, s2) == (UP, TODO): return WARN
	if (s1, s2) == (DOWN, DOWN): return DOWN
	if (s1, s2) == (DOWN, WARN): return WARN
	if (s1, s2) == (DOWN, NA): return DOWN
	if (s1, s2) == (DOWN, TODO): return WARN
	if (s1, s2) == (WARN, WARN): return WARN
	if (s1, s2) == (WARN, NA): return WARN
	if (s1, s2) == (WARN, TODO): return WARN
	if (s1, s2) == (NA, NA): return NA
	if (s1, s2) == (NA, TODO): return WARN
	if (s1, s2) == (TODO, TODO): return TODO
	return _merge(s2, s1)

class Status:
	"""Class that wraps printing and calculation of resource status
	"""
	status = 0

	def str(self, s):
		if s == UP: return 'UP'
		if s == DOWN: return 'DOWN'
		if s == WARN: return 'WARN'
		if s == NA: return 'N/A'
		if s == TODO: return 'TODO'

	def reset(self):
		self.status = 0

	def add(self, s):
		"""Merge a status with current global status
		"""
		self.status = _merge(self.status, s)
