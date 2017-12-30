"""
Converters, used by arguments and config file parsers.
"""
import re
import shlex

def convert_shlex(s):
    return shlex.split(s)

def convert_integer(s):
    """
    Return <s> cast to int.
    """
    return int(s)

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
        return s
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
    return re.split("\s*,\s*", s.strip())

def convert_set(s):
    """
    Return convert_list result cast to a set.
    """
    if isinstance(s, set):
        return s
    return set(convert_list(s))

def convert_set_comma(s):
    """
    Return convert_list_comma result cast to a set.
    """
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

def convert_duration(s, _to="s"):
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
        raise ValueError("convert duration error: None is not a valid duration")

    units = {
        "w": 604800,
        "d": 86400,
        "h": 3600,
        "m": 60,
        "s": 1,
    }

    if _to not in units:
        raise ValueError("convert duration error: unsupported target unit %s" % _to)

    try:
        s = int(s)
        return s // units[_to]
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
    if type(s) in (int, float):
        s = str(s)
    elif re.match("[0-9]+%(FREE|VG|ORIGIN|PVS)", s):
        # lvm2 size expressions
        return s
    l = ['', 'K', 'M', 'G', 'T', 'P', 'Z', 'E']
    s = s.strip().replace(",", ".")
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
    except:
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

def print_size(size, unit="MB"):
    mult = 1024
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'EB']
    units_index = {'B':0, 'KB':1, 'MB':2, 'GB':3, 'TB':4, 'EB':5}
    size = float(size)
    for u in units[units_index[unit]:]:
        if size == 0:
            return "0"
        if size < mult:
            return '%d%s' % (size, u.lower().rstrip("b"))
        size = size/mult
    return '%d%s' % (size, u.lower().rstrip("b"))

def convert_speed(s, _to='', _round=1, default_unit=''):
    try:
        s = s.rstrip(" /s")
    except AttributeError:
        pass
    size = convert_size(s, _to=_to, _round=_round, default_unit=default_unit)
    return size

def convert_speed_kps(s, _round=1):
    return convert_speed(s, _to="KB", _round=_round, default_unit='K')

if __name__ == "__main__":
    #print(convert_size("10000 KiB", _to='MiB', _round=3))
    #print(convert_size("10M", _to='', _round=4096))
    for s in (1, "1", "1w1d1h1m1s", "1d", "1d1w", "2m2s", "Ad", "1dd", "-1", "-1s"):
        try:
            print(s, "=>", convert_duration(s, _to="d"))
        except ValueError as exc:
            print(exc)
    for s in (3000, "3000 kb/s", "3000 k/s", "3000k"):
        try:
            print(s, "=>", convert_speed_kps(s))
        except ValueError as exc:
            print(exc)

