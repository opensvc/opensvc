"""
volmgr command line actions and options
"""
import commands.svcmgr.parser as mp

PROG = "volmgr"
ACTIONS = mp.ACTIONS
OPT = mp.OPT

class VolmgrOptParser(mp.SvcmgrOptParser):
    pass
