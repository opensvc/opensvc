import os
import platform
import rcExceptions as ex

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
    if use_color == "never" or (use_color == "auto" and not os.isatty(1)):
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
    s = re.sub(r'(")(error|ok|err|up|down|warn|n/a)(")', lambda m: m.group(1)+colorize_status(m.group(2), lpad=0)+m.group(3), s)
    s = re.sub(r'((?!"DEFAULT":)("[\w: ,@-]+":))', colorize(r'\1', color.LIGHTBLUE), s)
    s = re.sub(r'("DEFAULT":)( {)', colorize(r'\1', color.BROWN)+r'\2', s)
    s = re.sub(r'("[\w:-]+#[\w:-]+":)( {)', colorize(r'\1', color.BROWN)+r'\2', s)
    s = re.sub(r'(@[\w-]+)(":)', colorize(r'\1', color.RED)+colorize(r'\2', color.LIGHTBLUE), s)
    s = re.sub(r'({.+})', colorize(r'\1', color.GREEN), s)
    return s

def format_json(d):
    import json
    print(colorize_json(json.dumps(d, ensure_ascii=False, indent=4, separators=(',', ': '))))

def format_table(d):
    from rcPrintTable import print_table_tabulate
    print_table_tabulate(d)

def format_default(d):
    from rcPrintTable import print_table_default
    print_table_default(d)

def format_csv(d):
    from rcPrintTable import print_table_csv
    print_table_csv(d)

def is_list_dataset(d):
    if type(d) != list:
        return False
    if len(d) == 2 and type(d[0]) == list and type(d[1]) == list:
        return True
    if len(d) > 0 and type(d[0]) == list:
        return True
    return False

def is_dict_of_list_dataset(d):
    if type(d) != dict:
        return False
    for k, v in d.items():
        if not is_list_dataset(v):
            return False
    return True

def expand_list_dataset(d):
    if is_list_dataset(d):
        return _expand_list_dataset(d)
    if is_dict_of_list_dataset(d):
        for k in d:
            d[k] = _expand_list_dataset(d[k])
    return d

def _expand_list_dataset(d):
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
            raise ex.excError("unsupport format: %s" % str(fmt))

        data = fn(*args, **kwargs)
        if fmt == "json":
            data = expand_list_dataset(data)
        elif type(data) in (int, float):
            return
        elif len(data) == 0:
            return

        if data is None:
            return
        if type(data) != dict and type(data) != list:
            print(data)
            return

        _fmt(data)

    return decorator

