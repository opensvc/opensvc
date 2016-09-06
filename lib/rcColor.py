import os
import platform

use_color = "auto"

class color:
    WHITE = '\033[97m'
    BGGRAY = '\033[100m'
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    RED = '\033[31m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

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
    _colorize = win_colorize
else:
    _colorize = ansi_colorize

