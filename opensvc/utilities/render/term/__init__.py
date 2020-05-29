import os
import re

from env import Env
from utilities.proc import justcall, which

def term_width():
    min_columns = 78
    detected_columns = _detect_term_width()
    if detected_columns >= min_columns:
        return detected_columns
    else:
        env_columns = int(os.environ.get("COLUMNS", 0))
        if env_columns >= min_columns:
            return env_columns
        else:
            return min_columns


def _detect_term_width():
    try:
        # python 3.3+
        return os.get_terminal_size().columns
    except (AttributeError, OSError):
        pass
    if Env.sysname != "Windows" and which("stty") is not None:
        out, err, ret = justcall(['stty', '-a'])
        if ret == 0:
            m = re.search(r'columns\s+(?P<columns>\d+);', out)
            if m:
                return int(m.group('columns'))
    return 0
