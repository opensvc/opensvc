"""
This module defines the Status class and the functions
to convert a Status to its printable form or integer form.
"""

from utilities.render.color import colorize, color

UP = 0
DOWN = 1
WARN = 2
NA = 3
UNDEF = 5
STDBY_UP = 6
STDBY_DOWN = 7
STDBY_UP_WITH_UP = 8
STDBY_UP_WITH_DOWN = 9
STATUS_VALUE = {
    'up': UP,
    'down': DOWN,
    'warn': WARN,
    'n/a': NA,
    'na': NA,
    'undef': UNDEF,
    'stdby up': STDBY_UP,
    'stdby down': STDBY_DOWN,
}
STATUS_STR = {
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


def encode_pair(status1, status2):
    """
    Return a hashable code unique for the set([status1, status2]).
    """
    return (1 << status1) | (1 << status2)


MERGE_RULES = {
    encode_pair(UP, UP): UP,
    encode_pair(UP, DOWN): WARN,
    encode_pair(UP, WARN): WARN,
    encode_pair(UP, NA): UP,
    encode_pair(UP, STDBY_UP): STDBY_UP_WITH_UP,
    encode_pair(UP, STDBY_DOWN): WARN,
    encode_pair(UP, STDBY_UP_WITH_UP): STDBY_UP_WITH_UP,
    encode_pair(UP, STDBY_UP_WITH_DOWN): WARN,
    encode_pair(DOWN, DOWN): DOWN,
    encode_pair(DOWN, WARN): WARN,
    encode_pair(DOWN, NA): DOWN,
    encode_pair(DOWN, STDBY_UP): STDBY_UP_WITH_DOWN,
    encode_pair(DOWN, STDBY_DOWN): STDBY_DOWN,
    encode_pair(DOWN, STDBY_UP_WITH_UP): WARN,
    encode_pair(DOWN, STDBY_UP_WITH_DOWN): STDBY_UP_WITH_DOWN,
    encode_pair(WARN, WARN): WARN,
    encode_pair(WARN, NA): WARN,
    encode_pair(WARN, STDBY_UP): WARN,
    encode_pair(WARN, STDBY_DOWN): WARN,
    encode_pair(WARN, STDBY_UP_WITH_UP): WARN,
    encode_pair(WARN, STDBY_UP_WITH_DOWN): WARN,
    encode_pair(NA, NA): NA,
    encode_pair(NA, STDBY_UP): STDBY_UP,
    encode_pair(NA, STDBY_DOWN): STDBY_DOWN,
    encode_pair(NA, STDBY_UP_WITH_UP): STDBY_UP_WITH_UP,
    encode_pair(NA, STDBY_UP_WITH_DOWN): STDBY_UP_WITH_DOWN,
    encode_pair(STDBY_UP, STDBY_UP): STDBY_UP,
    encode_pair(STDBY_UP, STDBY_DOWN): WARN,
    encode_pair(STDBY_UP, STDBY_UP_WITH_UP): STDBY_UP_WITH_UP,
    encode_pair(STDBY_UP, STDBY_UP_WITH_DOWN): STDBY_UP_WITH_DOWN,
    encode_pair(STDBY_DOWN, STDBY_DOWN): STDBY_DOWN,
    encode_pair(STDBY_DOWN, STDBY_UP_WITH_UP): WARN,
    encode_pair(STDBY_DOWN, STDBY_UP_WITH_DOWN): WARN,
    encode_pair(STDBY_UP_WITH_UP, STDBY_UP_WITH_DOWN): WARN,
    encode_pair(STDBY_UP_WITH_UP, STDBY_UP_WITH_UP): STDBY_UP_WITH_UP,
    encode_pair(STDBY_UP_WITH_DOWN, STDBY_UP_WITH_DOWN): STDBY_UP_WITH_DOWN,
}


def colorize_status(status, lpad=10, agg_status=None):
    """
    Return the colorized human readable status string.
    """
    if isinstance(status, Status):
        status = str(status)
    elif isinstance(status, int):
        status = str(Status(status))

    fmt = "%-"+str(lpad)+"s"
    if status is None:
        return colorize(fmt % "undef", color.LIGHTBLUE)
    elif status == "warn":
        return colorize(fmt % status, color.BROWN)
    elif status == "down" or status in ("err", "error"):
        if agg_status == "up":
            return colorize(fmt % status, color.LIGHTBLUE)
        else:
            return colorize(fmt % status, color.RED)
    elif status == "up" or status == "ok":
        return colorize(fmt % status, color.GREEN)
    elif status == "stdby up":
        if agg_status == "up":
            return colorize(fmt % status, color.LIGHTBLUE)
        else:
            return colorize(fmt % status, color.RED)
    elif status == "stdby down":
        return colorize(fmt % status, color.RED)
    elif status == "n/a":
        return colorize(fmt % status, color.LIGHTBLUE)
    else:
        return colorize(fmt % status, color.LIGHTBLUE)


def status_value(status):
    """
    Return the machine readable status integer code.
    """
    if status not in STATUS_VALUE:
        return
    return STATUS_VALUE[status.lower()]


def status_str(val):
    """
    Return the human readable status string.
    """
    if val not in STATUS_STR:
        return
    return STATUS_STR[val]


class Status(object):
    """
    Class that wraps printing and calculation of resource status
    """
    @staticmethod
    def _merge(status1, status2):
        """
        Merge two status: WARN taints UP and DOWN
        """
        if status1 not in STATUS_STR:
            raise Exception("left member has unsupported value: %s" % str(status1))
        elif status2 not in STATUS_STR:
            raise Exception("right member has unsupported value: %s" % str(status2))

        if status1 == UNDEF:
            return status2
        elif status2 == UNDEF:
            return status1

        setstate = encode_pair(status1, status2)
        if setstate not in MERGE_RULES:
            raise Exception("some member has unsupported value: %s , %s " %
                            (str(status1), str(status2)))
        return MERGE_RULES[setstate]

    def value(self):
        """
        Return the integer status code.
        """
        return self.status

    def reset(self):
        """
        Reset the status to 'undef'.
        """
        self.status = UNDEF

    def __hash__(self):
        return hash(self.status)

    def __add__(self, other):
        self.status = self._merge(self.status, other.status)
        return self

    def __iadd__(self, other):
        if isinstance(other, Status):
            self.status = self._merge(self.status, other.status)
        else:
            self.status = self._merge(self.status, other)
        return self

    def __eq__(self, other):
        if isinstance(other, Status):
            return self.status == other.status
        try:
            other = int(other)
            return self.status == other
        except (ValueError, TypeError):
            pass
        return str(self) == other

    def __int__(self):
        return self.status

    def __str__(self):
        return status_str(self.status)

    def __init__(self, initial_status=None):
        if isinstance(initial_status, Status):
            self.status = initial_status.status
        elif isinstance(initial_status, int):
            self.status = initial_status
        elif initial_status is None:
            self.status = UNDEF
        else:
            try:
                self.status = int(initial_status)
            except (ValueError, TypeError):
                self.status = STATUS_VALUE[str(initial_status)]
