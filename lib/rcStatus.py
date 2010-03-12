#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import os

UP = 0
DOWN = 1
WARN = 2
NA = 3
TODO = 4
UNDEF = 5
STDBY_UP = 6
STDBY_DOWN = 7
STDBY_UP_WITH_UP = 8
STDBY_UP_WITH_DOWN = 9

GREEN = 32
RED = 31
YELLOW = 33

def colorize(color, text):
    #if os.isatty(1):
    #    return '\033['+str(color)+'m'+text+'\033[m'
    #else:
    return text

_status_value = {
    'up': UP,
    'down': DOWN,
    'warn': WARN,
    'n/a': NA,
    'na': NA,
    'todo': TODO,
    'undef': UNDEF,
    'stdby up': STDBY_UP,
    'stdby down': STDBY_DOWN,
}

_status_str = {
    UP: colorize(GREEN, 'up'),
    DOWN: colorize(RED, 'down'),
    WARN: colorize(YELLOW, 'warn'),
    NA: 'n/a',
    TODO: 'todo',
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
    """Merge too status: WARN and TODO taint UP and DOWN
    """
    if s1 not in _status_str:
        raise Exception("left member has unsupported value: %s"%str(s1))
    if s2 not in _status_str:
        raise Exception("right member has unsupported value: %s"%str(s2))

    if s1 == UNDEF: return s2
    if (s1, s2) == (UP, UP): return UP
    if (s1, s2) == (UP, DOWN): return WARN
    if (s1, s2) == (UP, WARN): return WARN
    if (s1, s2) == (UP, NA): return UP
    if (s1, s2) == (UP, TODO): return WARN
    if (s1, s2) == (UP, STDBY_UP): return STDBY_UP_WITH_UP
    if (s1, s2) == (UP, STDBY_DOWN): return WARN
    if (s1, s2) == (UP, STDBY_UP_WITH_UP): return STDBY_UP_WITH_UP
    if (s1, s2) == (UP, STDBY_UP_WITH_DOWN): return WARN
    if (s1, s2) == (DOWN, DOWN): return DOWN
    if (s1, s2) == (DOWN, WARN): return WARN
    if (s1, s2) == (DOWN, NA): return DOWN
    if (s1, s2) == (DOWN, TODO): return WARN
    if (s1, s2) == (DOWN, STDBY_UP): return STDBY_UP_WITH_DOWN
    if (s1, s2) == (DOWN, STDBY_DOWN): return WARN
    if (s1, s2) == (DOWN, STDBY_UP_WITH_UP): return WARN
    if (s1, s2) == (DOWN, STDBY_UP_WITH_DOWN): return STDBY_UP_WITH_DOWN
    if (s1, s2) == (WARN, WARN): return WARN
    if (s1, s2) == (WARN, NA): return WARN
    if (s1, s2) == (WARN, TODO): return WARN
    if (s1, s2) == (WARN, STDBY_UP): return WARN
    if (s1, s2) == (WARN, STDBY_DOWN): return WARN
    if (s1, s2) == (WARN, STDBY_UP_WITH_UP): return WARN
    if (s1, s2) == (WARN, STDBY_UP_WITH_DOWN): return WARN
    if (s1, s2) == (NA, NA): return NA
    if (s1, s2) == (NA, TODO): return WARN
    if (s1, s2) == (NA, STDBY_UP): return STDBY_UP
    if (s1, s2) == (NA, STDBY_DOWN): return STDBY_DOWN
    if (s1, s2) == (NA, STDBY_UP_WITH_UP): return STDBY_UP_WITH_UP
    if (s1, s2) == (NA, STDBY_UP_WITH_DOWN): return STDBY_UP_WITH_DOWN
    if (s1, s2) == (TODO, TODO): return TODO
    if (s1, s2) == (TODO, STDBY_UP): return TODO
    if (s1, s2) == (TODO, STDBY_DOWN): return TODO
    if (s1, s2) == (TODO, STDBY_UP_WITH_UP): return TODO
    if (s1, s2) == (TODO, STDBY_UP_WITH_DOWN): return TODO
    if (s1, s2) == (STDBY_UP, STDBY_UP): return STDBY_UP
    if (s1, s2) == (STDBY_UP, STDBY_DOWN): return WARN
    if (s1, s2) == (STDBY_UP, STDBY_UP_WITH_UP): return STDBY_UP_WITH_UP
    if (s1, s2) == (STDBY_UP, STDBY_UP_WITH_DOWN): return STDBY_UP_WITH_DOWN
    if (s1, s2) == (STDBY_DOWN, STDBY_DOWN): return WARN
    if (s1, s2) == (STDBY_DOWN, STDBY_DOWN): return WARN
    if (s1, s2) == (STDBY_DOWN, STDBY_UP_WITH_UP): return WARN
    if (s1, s2) == (STDBY_DOWN, STDBY_UP_WITH_DOWN): return WARN
    if (s1, s2) == (STDBY_UP_WITH_UP, STDBY_UP_WITH_DOWN): return WARN
    return _merge(s2, s1)

def print_status(resource, status):
    import string
    print '{0:70} {1}'.format(resource, status_str(status))

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
        self.status = _merge(self.status, s)
        return self

    def __str__(self):
        return status_str(self.status)

    def __init__(self, initial_status=UNDEF):
        self.status = initial_status
