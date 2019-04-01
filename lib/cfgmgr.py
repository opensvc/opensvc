# coding: utf8

import sys
from cfgmgr_parser import CfgmgrOptParser
import mgr

class Mgr(mgr.Mgr):
    def __init__(self, node=None):
        mgr.Mgr.__init__(self, parser=CfgmgrOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

