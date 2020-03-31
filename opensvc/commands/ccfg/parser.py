"""
The cluster configuration management command actions and options.
"""
import commands.mgr.parser as mp
from core.objects.svc import ACTION_ASYNC
from utilities.optparser import OptParser

PROG = "om cluster"

OPT = mp.OPT
ACTIONS = mp.ACTIONS

DEPRECATED_OPTIONS = [
]

DEPRECATED_ACTIONS = [
]

ACTIONS_TRANSLATIONS = {
}

class CcfgOptParser(OptParser):
    """
    The cluster configuration management command options parser class.
    """
    def __init__(self, args=None, colorize=True, width=None, formatter=None,
                 indent=6):
        OptParser.__init__(self, args=args, prog=PROG, options=OPT,
                           actions=ACTIONS,
                           deprecated_options=DEPRECATED_OPTIONS,
                           deprecated_actions=DEPRECATED_ACTIONS,
                           actions_translations=ACTIONS_TRANSLATIONS,
                           global_options=mp.GLOBAL_OPTS,
                           svc_select_options=mp.SVC_SELECT_OPTS,
                           colorize=colorize, width=width,
                           formatter=formatter, indent=indent, async_actions=ACTION_ASYNC)

