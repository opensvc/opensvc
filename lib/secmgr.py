# coding: utf8

import sys
from secmgr_parser import SecmgrOptParser
import mgr

class Mgr(mgr.Mgr):
    def __init__(self, node=None):
        mgr.Mgr.__init__(self, parser=SecmgrOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

