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
    if s2 == UNDEF: return s1
    setstate = set([s1, s2])
    if setstate == set([ UP, UP ]): return UP
    if setstate == set([ UP, DOWN ]): return WARN
    if setstate == set([ UP, WARN ]): return WARN
    if setstate == set([ UP, NA ]): return UP
    if setstate == set([ UP, TODO ]): return WARN
    if setstate == set([ UP, STDBY_UP ]): return STDBY_UP_WITH_UP
    if setstate == set([ UP, STDBY_DOWN ]): return WARN
    if setstate == set([ UP, STDBY_UP_WITH_UP ]): return STDBY_UP_WITH_UP
    if setstate == set([ UP, STDBY_UP_WITH_DOWN ]): return WARN
    if setstate == set([ DOWN, DOWN ]): return DOWN
    if setstate == set([ DOWN, WARN ]): return WARN
    if setstate == set([ DOWN, NA ]): return DOWN
    if setstate == set([ DOWN, TODO ]): return WARN
    if setstate == set([ DOWN, STDBY_UP ]): return STDBY_UP_WITH_DOWN
    if setstate == set([ DOWN, STDBY_DOWN ]): return WARN
    if setstate == set([ DOWN, STDBY_UP_WITH_UP ]): return WARN
    if setstate == set([ DOWN, STDBY_UP_WITH_DOWN ]): return STDBY_UP_WITH_DOWN
    if setstate == set([ WARN, WARN ]): return WARN
    if setstate == set([ WARN, NA ]): return WARN
    if setstate == set([ WARN, TODO ]): return WARN
    if setstate == set([ WARN, STDBY_UP ]): return WARN
    if setstate == set([ WARN, STDBY_DOWN ]): return WARN
    if setstate == set([ WARN, STDBY_UP_WITH_UP ]): return WARN
    if setstate == set([ WARN, STDBY_UP_WITH_DOWN ]): return WARN
    if setstate == set([ NA, NA ] ): return NA
    if setstate == set([ NA, TODO ]): return WARN
    if setstate == set([ NA, STDBY_UP ]): return STDBY_UP
    if setstate == set([ NA, STDBY_DOWN ]): return STDBY_DOWN
    if setstate == set([ NA, STDBY_UP_WITH_UP ]): return STDBY_UP_WITH_UP
    if setstate == set([ NA, STDBY_UP_WITH_DOWN ]): return STDBY_UP_WITH_DOWN
    if setstate == set([ TODO, TODO ]): return TODO
    if setstate == set([ TODO, STDBY_UP ]): return TODO
    if setstate == set([ TODO, STDBY_DOWN ]): return TODO
    if setstate == set([ TODO, STDBY_UP_WITH_UP ]): return TODO
    if setstate == set([ TODO, STDBY_UP_WITH_DOWN ]): return TODO
    if setstate == set([ STDBY_UP, STDBY_UP ]): return STDBY_UP
    if setstate == set([ STDBY_UP, STDBY_DOWN ]): return WARN
    if setstate == set([ STDBY_UP, STDBY_UP_WITH_UP ]): return STDBY_UP_WITH_UP
    if setstate == set([ STDBY_UP, STDBY_UP_WITH_DOWN ]): return STDBY_UP_WITH_DOWN
    if setstate == set([ STDBY_DOWN, STDBY_DOWN ]): return WARN
    if setstate == set([ STDBY_DOWN, STDBY_DOWN ]): return WARN
    if setstate == set([ STDBY_DOWN, STDBY_UP_WITH_UP ]): return WARN
    if setstate == set([ STDBY_DOWN, STDBY_UP_WITH_DOWN ]): return WARN
    if setstate == set([ STDBY_UP_WITH_UP, STDBY_UP_WITH_DOWN ]): return WARN
    if setstate == set([ STDBY_UP_WITH_UP, STDBY_UP_WITH_UP ]): return STDBY_UP_WITH_UP
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
        self.status = initial_status
