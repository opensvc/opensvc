import os
import platform

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
    s = re.sub(r'((?!"DEFAULT":)("[\w: @-]+":))', colorize(r'\1', color.LIGHTBLUE), s)
    s = re.sub(r'("DEFAULT":)( {)', colorize(r'\1', color.BROWN)+r'\2', s)
    s = re.sub(r'("[\w:-]+#[\w:-]+":)( {)', colorize(r'\1', color.BROWN)+r'\2', s)
    s = re.sub(r'(@[\w-]+)(":)', colorize(r'\1', color.RED)+colorize(r'\2', color.LIGHTBLUE), s)
    s = re.sub(r'({.+})', colorize(r'\1', color.GREEN), s)
    s = re.sub(r'"(ok|err|up|down|warn|n/a)"', lambda m: '"'+colorize_status(m.group(1), lpad=0)+'"', s)
    return s

