"""
The volume management command actions and options.
"""
import commands.svc.parser as mp

PROG = "om vol"
ACTIONS = mp.ACTIONS
OPT = mp.OPT

class VolOptParser(mp.SvcOptParser):
    """
    The volume management command options parser class.
    """
    pass
