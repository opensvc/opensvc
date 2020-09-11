# coding: utf8

import sys
from commands.nscfg.parser import NscfgOptParser
from commands.mgr import Mgr as BaseMgr

class Mgr(BaseMgr):
    def __init__(self, node=None):
        super(Mgr, self).__init__(parser=NscfgOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

