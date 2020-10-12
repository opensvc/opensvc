"""
Converters, used by arguments and config file parsers.
"""
import re
import shlex
import datetime
import json

import foreign.six as six

try:
    NUMERIC_TYPES = (int, float, long)
except NameError:
    NUMERIC_TYPES = (int, float)


def convert_datetime(s):
    if s is None:
        return
    if isinstance(s, datetime.datetime):
        return s
    if isinstance(s, datetime.date):
        return datetime.datetime(s)
    s = str(s)
    mask = "1970-01-01 00:00:00"
    length = len(s)
    if length > 19:
        s = s[:19]
    try:
        s = s + mask[length:]
    except IndexError:
        raise ValueError("unsupported datetime format %s. expect YYYY-MM-DD "
                         "HH:MM:SS, or right trimmed substring of")
    s = re.sub(r"\s+", ".", s)
    s = s.replace(":", ".")
    s = s.replace("-", ".")
    return datetime.datetime.strptime(s, "%Y.%m.%d.%H.%M.%S")


def convert_json(s):
    try:
        return json.loads(s)
    except Exception:
        return


def convert_shlex(s):
    if s is None:
        return
    if isinstance(s, list):
        return s
    if six.PY2:
        return shlex.split(s.encode("utf-8"))
    else:
        return shlex.split(s)


def convert_expanded_shlex(s):
    """
    Like the shlex converter but expands -it into -i -t
    """
    args = convert_shlex(s)
    if s is None:
        return
    new_args = []
    for arg in args:
        if arg and len(arg) > 2 and "=" not in arg and arg[0] == "-" and arg[1] != "-":
            for flag in arg[1:]:
                new_args.append("-" + flag)
        else:
            new_args.append(arg)
    return new_args


def convert_lower(s):
    """
    Return <s> lowercased
    """
    if s is None:
        return
    return s.lower()


def convert_integer(s):
    """
    Return <s> cast to int.
    """
    if s is None:
        return
    try:
        return int(float(s))
    except ValueError:
        return


def convert_list(s):
    """
    Return a list object from the <s>.
    Consider <s> is white-space delimited.
    """
    if s is None:
        return []
    if isinstance(s, list):
        return s
    return s.split()


def convert_list_lower(s):
    """
    Return convert_list with members converted to lowercase.
    """
    if s is None:
        return []
    if isinstance(s, list):
        return [member.lower() for member in s]
    return [member.lower() for member in convert_list(s)]


def convert_list_comma(s):
    """
    Return a list object from the <s>.
    Consider <s> is comma delimited.
    """
    if s is None:
        return []
    if isinstance(s, list):
        return s
    return [word for word in re.split(r"\s*,\s*", s.strip()) if word != ""]


def convert_set(s):
    """
    Return convert_list result cast to a set.
    """
    if s is None:
        return set()
    if isinstance(s, set):
        return s
    return set(convert_list(s))


def convert_set_comma(s):
    """
    Return convert_list_comma result cast to a set.
    """
    if s is None:
        return set()
    if isinstance(s, set):
        return s
    return set(convert_list_comma(s))


def convert_boolean(s):
    """
    Return a boolean from <s>.
    """
    true_vals = (
        "yes",
        "y",
        "true",
        "t",
        "1"
    )
    false_vals = (
        "no",
        "n",
        "false",
        "f",
        "0",
        "0.0",
        "",
        "none",
        "[]",
        "{}"
    )
    s = str(s).lower()
    if s in true_vals:
        return True
    if s in false_vals:
        return False
    raise ValueError('convert boolean error: ' + s)


def convert_tristate(s):
    """
    A tri-state returns None for None, True for true values,
    False for false values.
    """
    if s is None:
        return
    return convert_boolean(s)


def convert_duration_minute(s):
    return convert_duration(s, _from="m")


def convert_duration_to_day(s):
    return convert_duration(s, _to="d")


def convert_duration(s, _to="s", _from="s"):
    """
    Convert a string representation of a duration to seconds.
    Supported units (case insensitive):
      w: week
      d: day
      h: hour
      m: minute
      s: second
    Example:
      1w => 604800
      1d => 86400
      1h => 3600
      1h1m => 3660
      1h2s => 3602
      1 => 1
    """
    if s is None:
        return

    units = {
        "w": 604800,
        "d": 86400,
        "h": 3600,
        "m": 60,
        "s": 1,
    }

    if _from not in units:
        raise ValueError("convert duration error: unsupported input unit %s" % _from)
    if _to not in units:
        raise ValueError("convert duration error: unsupported target unit %s" % _to)

    try:
        s = int(s)
        return s * units[_from] // units[_to]
    except ValueError:
        pass

    s = s.lower()
    duration = 0
    prev = 0
    for idx, unit in enumerate(s):
        if unit not in units:
            continue
        _duration = s[prev:idx]
        try:
            _duration = int(_duration)
        except ValueError:
            raise ValueError("convert duration error: invalid format %s at index %d" % (s, idx))
        duration += _duration * units[unit]
        prev = idx + 1

    return duration // units[_to]


def convert_size(s, _to='', _round=1, default_unit=''):
    """
    Return an integer from the <s> expression, converting to a pivot unit,
    then to the target unit specified by <_to>, and finally round to a
    <_round> lowest multiple.
    """
    if s is None:
        return
    if type(s) in NUMERIC_TYPES:
        s = str(s)
    elif re.match(r"[0-9]+%(FREE|VG|ORIGIN|PVS|)", s):
        # lvm2 size expressions or percentage
        return s
    l = ['', 'K', 'M', 'G', 'T', 'P', 'Z', 'E']
    s = s.strip().replace(",", ".")
    s = s.replace("B", "")
    if len(s) == 0:
        return 0
    if s == '0':
        return 0
    size = s
    unit = ""
    for i, c in enumerate(s):
        if not c.isdigit() and c not in ('.', '-'):
            size = s[:i]
            unit = s[i:].strip()
            break
    if 'i' in unit:
        factor = 1000
    else:
        factor = 1024
    if len(unit) > 0:
        unit = unit[0].upper()
    else:
        unit = default_unit
    size = float(size)

    try:
        start_idx = l.index(unit)
    except ValueError:
        raise ValueError("convert size error: unsupported unit in %s" % s)

    for i in range(start_idx):
        size *= factor

    if 'i' in _to:
        factor = 1000
    else:
        factor = 1024
    if len(_to) > 0:
        unit = _to[0].upper()
    else:
        unit = ''

    if unit == 'B':
        unit = ''

    try:
        end_idx = l.index(unit)
    except:
        raise ValueError("convert size error: unsupported target unit %s" % unit)

    for i in range(end_idx):
        size /= factor

    size = int(size)
    d = size % _round
    if d > 0:
        size = (size // _round) * _round
    return size


def print_size(size, unit="MB", compact=False, precision=3):
    unit = unit.upper()
    if unit.endswith("B"):
        suffix = "B"
        unit = unit.rstrip("B")
    else:
        suffix = ""
    if unit.endswith("I"):
        metric = "i"
        mult = 1000
        unit = unit.rstrip("I")
    else:
        metric = ""
        mult = 1024
    units = ['', 'K', 'M', 'G', 'T', 'E', 'Z', 'Y']
    units_index = {'': 0, 'K': 1, 'M': 2, 'G': 3, 'T': 4, 'E': 5, 'Z': 6, 'Y': 7}
    size = float(size)
    if unit not in units:
        raise ValueError("unsupported unit: %s" % unit)
    sep = "" if compact else " "
    suffix = "" if compact else suffix
    roundup = False
    done = False
    for u in units[units_index[unit]:]:
        if size == 0:
            return "0"
        u = u.lower() if compact else u
        if size < mult:
            done = True
            break
        size = size / mult
    if not done:
        size *= mult
    ufmt = "%d"
    for exp in range(0, precision - 1):
        if size < 10 ** (exp + 1):
            ufmt = "%." + str(precision - 1 - exp) + "f"
            break
    return (ufmt + '%s%s%s%s') % (size, sep, u, metric, suffix)


def convert_speed(s, _to='', _round=1, default_unit=''):
    try:
        s = s.rstrip(" /s")
    except AttributeError:
        pass
    size = convert_size(s, _to=_to, _round=_round, default_unit=default_unit)
    return size


def convert_speed_kps(s, _round=1):
    return convert_speed(s, _to="KB", _round=_round, default_unit='K')


def print_duration(secs, _round=2):
    buff = ""
    idx = 0
    if secs >= 86400:
        n = secs // 86400
        secs = secs % 86400
        buff += "%dd" % n
        idx += 1
        if _round and idx >= _round:
            return buff
    if secs >= 3600:
        n = secs // 3600
        secs = secs % 3600
        buff += "%02dh" % n
        idx += 1
        if _round and idx >= _round:
            return buff
    if secs >= 60:
        n = secs // 60
        secs = secs % 60
        buff += "%02dm" % n
        idx += 1
        if _round and idx >= _round:
            return buff[:-1]
    if secs > 0:
        buff += "%05.2f" % secs
        if idx == 0:
            if buff[3:] == "00":
                return buff[:2] + "s"
            else:
                return buff[:2] + "s" + buff[3:]
        else:
            return buff[:-3]
    if buff:
        return buff
    return "-"


if __name__ == "__main__":
    print(print_duration(92000))
    print(print_duration(86400))
    print(print_duration(9200))
    print(print_duration(1))
