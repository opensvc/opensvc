import os
import platform
from rcColor import color, colorize

UP = 0
DOWN = 1
WARN = 2
NA = 3
UNDEF = 5
STDBY_UP = 6
STDBY_DOWN = 7
STDBY_UP_WITH_UP = 8
STDBY_UP_WITH_DOWN = 9

def colorize_status(s, lpad=10):
    if type(s) == Status:
        s = str(s)
    fmt = "%-"+str(lpad)+"s"
    if s == "warn":
        return colorize(fmt%s, color.BROWN)
    elif s.endswith("down") or s in ("err", "error"):
        return colorize(fmt%s, color.RED)
    elif s.endswith("up") or s == "ok":
        return colorize(fmt%s, color.GREEN)
    elif s == "n/a":
        return colorize(fmt%s, color.LIGHTBLUE)
    return fmt%s

_status_value = {
    'up': UP,
    'down': DOWN,
    'warn': WARN,
    'n/a': NA,
    'na': NA,
    'undef': UNDEF,
    'stdby up': STDBY_UP,
    'stdby down': STDBY_DOWN,
}

_status_str = {
    UP: 'up',
    DOWN: 'down',
    WARN: 'warn',
    NA: 'n/a',
    UNDEF: 'undef',
    STDBY_UP: 'stdby up',
    STDBY_DOWN: 'stdby down',
    STDBY_UP_WITH_UP: 'up',
    STDBY_UP_WITH_DOWN: 'stdby up',
}

def status_value(str):
    if str not in _status_value.keys():
        return None
    return _status_value[str.lower()]

def status_str(val):
    if val not in _status_str.keys():
        return None
    return _status_str[val]

def _merge(s1, s2):
    """Merge two status: WARN taints UP and DOWN
    """
    if s1 not in _status_str:
        raise Exception("left member has unsupported value: %s"%str(s1))
    elif s2 not in _status_str:
        raise Exception("right member has unsupported value: %s"%str(s2))

    if s1 == UNDEF: return s2
    elif s2 == UNDEF: return s1
    setstate = set([s1, s2])
    if setstate == set([ UP, UP ]): return UP
    elif setstate == set([ UP, DOWN ]): return WARN
    elif setstate == set([ UP, WARN ]): return WARN
    elif setstate == set([ UP, NA ]): return UP
    elif setstate == set([ UP, STDBY_UP ]): return STDBY_UP_WITH_UP
    elif setstate == set([ UP, STDBY_DOWN ]): return WARN
    elif setstate == set([ UP, STDBY_UP_WITH_UP ]): return STDBY_UP_WITH_UP
    elif setstate == set([ UP, STDBY_UP_WITH_DOWN ]): return WARN
    elif setstate == set([ DOWN, DOWN ]): return DOWN
    elif setstate == set([ DOWN, WARN ]): return WARN
    elif setstate == set([ DOWN, NA ]): return DOWN
    elif setstate == set([ DOWN, STDBY_UP ]): return STDBY_UP_WITH_DOWN
    elif setstate == set([ DOWN, STDBY_DOWN ]): return STDBY_DOWN
    elif setstate == set([ DOWN, STDBY_UP_WITH_UP ]): return WARN
    elif setstate == set([ DOWN, STDBY_UP_WITH_DOWN ]): return STDBY_UP_WITH_DOWN
    elif setstate == set([ WARN, WARN ]): return WARN
    elif setstate == set([ WARN, NA ]): return WARN
    elif setstate == set([ WARN, STDBY_UP ]): return WARN
    elif setstate == set([ WARN, STDBY_DOWN ]): return WARN
    elif setstate == set([ WARN, STDBY_UP_WITH_UP ]): return WARN
    elif setstate == set([ WARN, STDBY_UP_WITH_DOWN ]): return WARN
    elif setstate == set([ NA, NA ] ): return NA
    elif setstate == set([ NA, STDBY_UP ]): return STDBY_UP
    elif setstate == set([ NA, STDBY_DOWN ]): return STDBY_DOWN
    elif setstate == set([ NA, STDBY_UP_WITH_UP ]): return STDBY_UP_WITH_UP
    elif setstate == set([ NA, STDBY_UP_WITH_DOWN ]): return STDBY_UP_WITH_DOWN
    elif setstate == set([ STDBY_UP, STDBY_UP ]): return STDBY_UP
    elif setstate == set([ STDBY_UP, STDBY_DOWN ]): return WARN
    elif setstate == set([ STDBY_UP, STDBY_UP_WITH_UP ]): return STDBY_UP_WITH_UP
    elif setstate == set([ STDBY_UP, STDBY_UP_WITH_DOWN ]): return STDBY_UP_WITH_DOWN
    elif setstate == set([ STDBY_DOWN, STDBY_DOWN ]): return STDBY_DOWN
    elif setstate == set([ STDBY_DOWN, STDBY_UP_WITH_UP ]): return WARN
    elif setstate == set([ STDBY_DOWN, STDBY_UP_WITH_DOWN ]): return WARN
    elif setstate == set([ STDBY_UP_WITH_UP, STDBY_UP_WITH_DOWN ]): return WARN
    elif setstate == set([ STDBY_UP_WITH_UP, STDBY_UP_WITH_UP ]): return STDBY_UP_WITH_UP
    elif setstate == set([ STDBY_UP_WITH_DOWN, STDBY_UP_WITH_DOWN ]): return STDBY_UP_WITH_DOWN
    raise Exception("some member has unsupported value: %s , %s "%(str(s1),str(s2)) )

class Status(object):
    """Class that wraps printing and calculation of resource status
    """
    def reset(self):
        self.status = UNDEF

    def __add__(self, s):
        self.status = _merge(self.status, s.status)
        return self

    def __iadd__(self, s):
        """Merge a status with current global status
        """
        if isinstance(s, Status):
            self.status = _merge(self.status, s.status)
        else:
            self.status = _merge(self.status, s)
        return self

    def __str__(self):
        return status_str(self.status)

    def __init__(self, initial_status=UNDEF):
        if type(initial_status) == int:
            self.status = initial_status
        elif type(initial_status) == Status:
            self.status = initial_status.status
        else:
            self.status = _status_value[str(initial_status)]
