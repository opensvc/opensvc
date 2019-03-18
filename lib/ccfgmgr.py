# coding: utf8

import sys
from ccfgmgr_parser import CcfgmgrOptParser
import mgr

class Mgr(mgr.Mgr):
    def __init__(self, node=None):
        mgr.Mgr.__init__(self, parser=CcfgmgrOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

