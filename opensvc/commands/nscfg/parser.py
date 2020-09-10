"""
The namespace configuration management command actions and options.
"""
import commands.mgr.parser as mp
import commands.svc.parser as svcp
from core.objects.svc import ACTION_ASYNC
from utilities.optparser import OptParser

PROG = "om nscfg"

OPT = mp.OPT
ACTIONS = mp.ACTIONS
ACTIONS["Object actions"] = svcp.PG_ACTIONS

DEPRECATED_OPTIONS = [
]

DEPRECATED_ACTIONS = [
]

ACTIONS_TRANSLATIONS = {
}

class NscfgOptParser(OptParser):
    """
    The namespace configuration management command options parser class.
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

