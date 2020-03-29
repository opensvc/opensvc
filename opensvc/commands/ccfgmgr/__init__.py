# coding: utf8

import sys
from commands.ccfgmgr.parser import CcfgmgrOptParser
from commands.mgr import Mgr as BaseMgr

class Mgr(BaseMgr):
    def __init__(self, node=None):
        super(Mgr, self).__init__(parser=CcfgmgrOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

