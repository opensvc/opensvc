from __future__ import print_function
import os
import sys
import platform
import rcExceptions as ex
from rcUtilities import is_string

use_color = "auto"

class color:
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    BROWN = '\033[33m'
    BLUE = '\033[34m'
    PURPLE = '\033[35m'
    CYAN = '\033[36m'
    GRAY = '\033[37m'

    DARKGRAY = '\033[90m'
    LIGHTRED = '\033[91m'
    LIGHTGREEN = '\033[92m'
    YELLOW = '\033[93m'
    LIGHTBLUE = '\033[94m'
    LIGHTPURPLE = '\033[95m'
    LIGHTCYAN = '\033[96m'
    WHITE = '\033[97m'

    BGGRAY = '\033[100m'

def ansi_colorize(s, c=None):
    global use_color
    if c is None:
        return s
    if use_color in ("never", "no") or (use_color == "auto" and not os.isatty(1)):
        return s
    return c + s + color.END

def win_colorize(s, c=None):
    return s

if platform.system() == 'Windows':
    colorize = win_colorize
else:
    colorize = ansi_colorize

def colorize_json(s):
    import re
    from rcStatus import colorize_status
    s = re.sub(r'(")(error|ok|err|up|down|warn|n/a|stdby up|stdby down)(")', lambda m: m.group(1)+colorize_status(m.group(2), lpad=0)+m.group(3), s)
    s = re.sub(r'((?!"DEFAULT":)("[\w: ,@-]+":))', colorize(r'\1', color.LIGHTBLUE), s)
    s = re.sub(r'("DEFAULT":)( {)', colorize(r'\1', color.BROWN)+r'\2', s)
    s = re.sub(r'("[\w:-]+#[\w:-]+":)( {)', colorize(r'\1', color.BROWN)+r'\2', s)
    s = re.sub(r'(@[\w-]+)(":)', colorize(r'\1', color.RED)+colorize(r'\2', color.LIGHTBLUE), s)
    s = re.sub(r'({.+})', colorize(r'\1', color.GREEN), s)
    return s

def format_json(d):
    import json
    kwargs = {
      "ensure_ascii": False,
      "indent": 4,
      "separators": (',', ': '),
    }
    if sys.version_info[0] < 3:
        kwargs["encoding"] = "utf8"
    print(colorize_json(json.dumps(d, **kwargs)))

def format_table(d):
    from rcPrintTable import print_table_tabulate
    print_table_tabulate(d)

def format_default(d):
    from rcPrintTable import print_table_default
    if "error" in d and is_string(d["error"]):
        print(d["error"], file=sys.stderr)
    print_table_default(d)

def format_csv(d):
    from rcPrintTable import print_table_csv
    print_table_csv(d)

def is_list_of_list(d):
    if type(d) != list:
        return False
    if len(d) == 2 and type(d[0]) == list and type(d[1]) == list:
        return True
    if len(d) > 0 and type(d[0]) == list:
        return True
    return False

def is_list_of_dict(d):
    if type(d) != list:
        return False
    if len(d) == 0:
        return False
    for e in d:
        if type(e) != dict:
            return False
    return True

def is_dict_of_list(d):
    if type(d) != dict:
        return False
    for k, v in d.items():
        if not is_list_of_list(v):
            return False
    return True

def is_dict_of_list_of_dict(d):
    if type(d) != dict:
        return False
    for k, v in d.items():
        if not is_list_of_dict(v):
            return False
    return True

def is_dict_of_list_of_list(d):
    if type(d) != dict:
        return False
    for k, v in d.items():
        if not is_list_of_list(v):
            return False
    return True

def xform_data_for_tabular(d):
    if is_list_of_dict(d):
        return _xform_ld_data_for_tabular(d)
    if is_dict_of_list_of_dict(d):
        return _xform_dld_data_for_tabular(d)
    if is_dict_of_list_of_list(d):
        return _xform_dll_data_for_tabular(d)
    return d

def _xform_dll_data_for_tabular(d):
    l = []
    for k, v in d.items():
        if len(v) == 0:
            continue
        v[0].insert(0, "service")
        for i, e in enumerate(v[1:]):
            v[i+1].insert(0, k)
        if len(l) == 0:
            l += v
        else:
            l += v[1:]
    return l

def _xform_dld_data_for_tabular(d):
    l = []
    for k, v in d.items():
        if len(l) == 0:
            l += _xform_ld_data_for_tabular(v, include_header=True, prepend=("service", k))
        else:
            l += _xform_ld_data_for_tabular(v, include_header=False, prepend=("service", k))
    return l

def _xform_ld_data_for_tabular(d, include_header=True, prepend=None):
    l = []
    if include_header:
        header = d[0].keys()
        if prepend:
            header.insert(0, prepend[0])
        l += [header]
    for e in d:
        values = e.values()
        if prepend:
            values.insert(0, prepend[1])
        l.append(values)
    return l
    
def xform_data_for_json(d):
    if is_list_of_list(d):
        return _xform_data_for_json(d)
    if is_dict_of_list(d):
        for k in d:
            d[k] = _xform_data_for_json(d[k])
    return d

def _xform_data_for_json(d):
    if len(d) < 2:
        return []
    l = []
    titles = d[0]
    for _d in d[1:]:
        h = {}
        for a, b in zip(titles, _d):
            h[a] = b
        l.append(h)
    return l

def formatter(fn):
    def decorator(*args, **kwargs):
        fmt = args[0].options.format

        if fmt == "json":
            _fmt = format_json
        elif fmt == "table":
            _fmt = format_table
        elif fmt == "csv":
            _fmt = format_csv
        elif fmt is None:
            _fmt = format_default
        elif hasattr(fmt, "__call__"):
            _fmt = fmt
        else:
            raise ex.excError("unsupported output format: %s" % str(fmt))

        data = fn(*args, **kwargs)

        if fmt == "json":
            data = xform_data_for_json(data)
        elif fmt in ("table", "csv", None):
            data = xform_data_for_tabular(data)

        if data is None:
            return
        if type(data) in (int, float):
            return
        if len(data) == 0:
            return

        if not isinstance(data, (dict, list)):
            print(data)
            return

        _fmt(data)

        if "error" in data:
            return 1

    return decorator

