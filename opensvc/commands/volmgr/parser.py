"""
The volume management command actions and options.
"""
import commands.svcmgr.parser as mp

PROG = "om vol"
ACTIONS = mp.ACTIONS
OPT = mp.OPT

class VolmgrOptParser(mp.SvcmgrOptParser):
    """
    The volume management command options parser class.
    """
    pass
