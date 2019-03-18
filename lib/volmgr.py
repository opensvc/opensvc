# coding: utf8

import sys
from svcmgr_parser import SvcmgrOptParser
import mgr

class Mgr(mgr.Mgr):
    def __init__(self, node=None):
        mgr.Mgr.__init__(self, parser=SvcmgrOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

