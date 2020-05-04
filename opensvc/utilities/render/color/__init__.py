from __future__ import print_function

import os
import sys
import datetime
import re

import core.exceptions as ex
import foreign.six as six
from utilities.string import is_string

try:
    from collections import OrderedDict
except ImportError:
    OrderedDict = dict

if os.name == "nt":
    import foreign.colorama as colorama
    colorama.init()

use_color = "auto"

unicons = {
    "frozen": "*",
    "stdby up": "S",
    "stdby down": "s",
    "up": "O",
    "down": "X",
    "warn": "!",
    "n/a": "/",
    "undef": "/",
}

class color:
    END = '\033[000m'
    BOLD = '\033[001m'
    UNDERLINE = '\033[004m'

    BLACK = '\033[030m'
    RED = '\033[031m'
    GREEN = '\033[032m'
    BROWN = '\033[033m'
    BLUE = '\033[034m'
    PURPLE = '\033[035m'
    CYAN = '\033[036m'
    GRAY = '\033[037m'

    DARKGRAY = '\033[090m'
    LIGHTRED = '\033[091m'
    LIGHTGREEN = '\033[092m'
    YELLOW = '\033[093m'
    LIGHTBLUE = '\033[094m'
    LIGHTPURPLE = '\033[095m'
    LIGHTCYAN = '\033[096m'
    WHITE = '\033[097m'

    BGBLACK = '\033[040m'
    BGRED = '\033[041m'
    BGGREEN = '\033[042m'
    BGYELLOW = '\033[043m'
    BGBLUE = '\033[044m'
    BGPURPLE = '\033[045m'
    BGCYAN = '\033[046m'
    BGWHITE = '\033[047m'
    BGDEFAULT = '\033[049m'
    BGGRAY = '\033[100m'

    E_BGODD = '\033[48;2;240;240;205m'
    E_BGCYAN = '\033[48;2;125;205;205m'

if os.environ.get("TERM") in ("screen-256color", "xterm-256color", "screen-256color-bce"):
    color.LIGHTBLUE = '\033[038;5;243m'

STATUS_COLOR = {
    "up": color.GREEN,
    "stdby up": color.GREEN,
    "ok": color.GREEN,
    "down": color.RED,
    "stdby down": color.RED,
    "err": color.RED,
    "error": color.RED,
    "n/a": color.LIGHTBLUE,
    "undef": color.LIGHTBLUE,
    "unknown": color.LIGHTBLUE,
    "warn": color.BROWN,
}

AUTO_COLORS = [
    color.BLUE,
    color.LIGHTRED,
    color.BROWN,
    color.LIGHTBLUE,
    color.PURPLE,
    color.CYAN,
    color.GREEN,
    color.YELLOW,
]
AUTO_COLORS_LEN = len(AUTO_COLORS)

def ansi_colorize(s, c=None):
    global use_color
    if c is None:
        return s
    if use_color in ("never", "no") or (use_color == "auto" and not os.isatty(1)):
        return s
    return c + s + color.END

colorize = ansi_colorize

def colorize_json(s):
    import re
    from core.status import colorize_status
    s = re.sub(r'(")(error|ok|err|up|down|warn|n/a|stdby up|stdby down)(")', lambda m: m.group(1)+colorize_status(m.group(2), lpad=0)+m.group(3), s)
    s = re.sub(r'((?!"DEFAULT":)("[\w: ,@-]+":))', colorize(r'\1', color.LIGHTBLUE), s)
    s = re.sub(r'("DEFAULT":)( {)', colorize(r'\1', color.BROWN)+r'\2', s)
    s = re.sub(r'("[\w:-]+#[\w\.:-]+":)( {)', colorize(r'\1', color.BROWN)+r'\2', s)
    s = re.sub(r'(@[\w-]+)(":)', colorize(r'\1', color.RED)+colorize(r'\2', color.LIGHTBLUE), s)
    s = re.sub(r'({.+})', colorize(r'\1', color.GREEN), s)
    return s

def format_json(d):
    import json

    sort_keys = not isinstance(d, OrderedDict) and OrderedDict != dict

    kwargs = {
      "sort_keys": sort_keys,
      "ensure_ascii": False,
      "indent": 4,
      "separators": (',', ': '),
    }
    if six.PY2:
        kwargs["encoding"] = "utf8"
    colorize_json(json.dumps(d, **kwargs))
    try:
        print(colorize_json(json.dumps(d, **kwargs)))
    except IOError:
        pass
    except:
        kwargs["ensure_ascii"] = True
        print(colorize_json(json.dumps(d, **kwargs)))

def format_flat_json(d):
    print(format_str_flat_json(d))

def format_str_flat_json(d):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a, v in x.items():
                a = str(a)
                if "/" in a or "#" in a or "." in a or "$" in a:
                    a = "'"+a+"'"
                flatten(v, name="%s.%s" % (name, a))
        elif isinstance(x, list):
            i = 0
            for a in x:
                flatten(a, name="%s[%d]" % (name, i))
                i += 1
        else:
            out[name] = x

    flatten(d)
    buff = ""
    for k, v in sorted(out.items(), key=lambda x: x[0]):
        buff += "%s = %s\n" % (k, v)
    if buff.endswith("\n"):
        buff = buff[:-1]
    if six.PY2:
        return buff.encode("utf-8")
    else:
        return buff

def format_table(d):
    import utilities.render.table
    utilities.render.table.print_table_tabulate(d)

def format_default(d):
    import utilities.render.table
    if "error" in d and is_string(d["error"]):
        print(d["error"], file=sys.stderr)
    utilities.render.table.print_table_default(d)

def format_csv(d):
    import utilities.render.table
    utilities.render.table.print_table_csv(d)

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

def flatten_list(data):
    for idx, entry in enumerate(data):
        if not isinstance(entry, dict):
            continue
        for key in [k for k in entry]:
            val = entry[key]
            if not isinstance(val, dict):
                continue
            for _key in [k for k in val]:
                _val = val[_key]
                agg_key = key + "." + _key
                data[idx][agg_key] = _val
            del data[idx][key]
    return data

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
    d = flatten_list(d)
    l = []
    if include_header:
        header = list(d[0].keys())
        if prepend:
            header.insert(0, prepend[0])
        l += [header]
    for e in d:
        values = list(e.values())
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
        default_fmt = kwargs.get("default_fmt")
        if fmt is None and default_fmt:
            fmt = default_fmt

        _fmt_kwargs = {}
        if fmt == "json":
            _fmt = format_json
        elif fmt == "flat_json":
            _fmt = format_flat_json
        elif fmt == "table":
            _fmt = format_table
        elif fmt == "csv":
            _fmt = format_csv
        elif fmt is None or fmt == "default":
            _fmt = format_default
        elif hasattr(fmt, "__call__"):
            _fmt = fmt
        else:
            raise ex.Error("unsupported output format: %s" % str(fmt))

        data = fn(*args, **kwargs)

        if fmt in ("json", "flat_json"):
            data = xform_data_for_json(data)
        elif fmt in ("table", "csv", "default", None):
            data = xform_data_for_tabular(data)

        if data is None:
            return
        if type(data) in (int, float):
            return
        if fmt != "json" and len(data) == 0:
            return

        if not isinstance(data, (dict, list)):
            print(data)
            return

        path = args[0].options.jsonpath_filter
        if path:
            from foreign.jsonpath_ng.ext import parse
            try:
                jsonpath_expr = parse(path)
                data = [match.value for match in jsonpath_expr.find(data)]
            except Exception as exc:
                raise ex.Error(str(exc))
            if re.match(r"^[\w\.']+$", path):
                # single value expression
                try:
                    data = data[0]
                except IndexError:
                    return

        if not isinstance(data, (dict, list)):
            print(data)
            return

        try:
            _fmt(data)
        except IOError as ioexc:
            if ioexc.errno == 32:
                # broken pipe (ex: tail, pager, ...)
                pass
            else:
                raise

        if "error" in data:
            return 1

    return decorator

def print_color_config(fpath):
    """
    Colorize and print the content of the file passed as argument.
    """
    def highlighter(line):
        """
        Colorize interesting parts to help readability
        """
        line = line.rstrip("\n")
        if re.match(r'\[.+\]', line):
            return colorize(line, color.BROWN)
        line = re.sub(
            r"({[\.\w\-_#{}\[\]()\$\+]+})",
            colorize(r"\1", color.GREEN),
            line
        )
        line = re.sub(
            r"^(\s*\w+\s*)=",
            colorize(r"\1", color.LIGHTBLUE)+"=",
            line
        )
        line = re.sub(
            r"^(\s*\w+)(@[\w\.-]+\s*)=",
            colorize(r"\1", color.LIGHTBLUE)+colorize(r"\2", color.RED)+"=",
            line
        )
        return line
    try:
        with open(fpath, 'r') as ofile:
            for line in ofile.readlines():
                print(highlighter(line))
    except Exception as exc:
        raise ex.Error(exc)

def colorize_log_line(line, last=None, auto=None):
    """
    Format a log line, colorizing the log level.
    Return the line as a string buffer.
    """
    msg = os.linesep.join(line["m"])
    extra = ""
    for k, v in line["x"].items():
        if k in ("n", "o") and auto:
            try:
                i = auto.index(v)
                v = colorize(v, AUTO_COLORS[i%AUTO_COLORS_LEN])
            except (ValueError, IndexError):
                pass
        extra += "%s:%s " % (k, v)
    extra = extra.rstrip()
    lvl = line["l"]
    t = datetime.datetime.fromtimestamp(line["t"]).strftime("%Y-%m-%d %H:%M:%S,%f")

    if lvl == "INFO":
        lvl = colorize("INFO   ", color.LIGHTBLUE)
    elif lvl == "WARNING":
        lvl = colorize("WARNING", color.BROWN)
    elif lvl == "ERROR":
        lvl = colorize("ERROR  ", color.RED)
    elif lvl == "DEBUG":
        lvl = "DEBUG  "

    if msg.startswith("do "):
        msg = colorize(msg, color.BOLD)

    if auto:
        for i, word in enumerate(auto):
            msg = re.sub(r"([\s,:@'\"]+)%s([\s,:@'\"]+)"%word, lambda m: m.group(1)+colorize(word, AUTO_COLORS[i%AUTO_COLORS_LEN])+m.group(2), msg)
    line = " ".join((t, lvl, extra, "|", msg))
    return line


