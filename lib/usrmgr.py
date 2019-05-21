# coding: utf8

import sys
from usrmgr_parser import UsrmgrOptParser
import mgr

class Mgr(mgr.Mgr):
    def __init__(self, node=None):
        mgr.Mgr.__init__(self, parser=UsrmgrOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

