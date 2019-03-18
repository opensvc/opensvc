# coding: utf8

import sys
from volmgr_parser import VolmgrOptParser
import mgr

class Mgr(mgr.Mgr):
    def __init__(self, node=None):
        mgr.Mgr.__init__(self, parser=VolmgrOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

