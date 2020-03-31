# coding: utf8

import sys
from commands.sec.parser import SecOptParser
from commands.mgr import Mgr as BaseMgr

class Mgr(BaseMgr):
    def __init__(self, node=None):
        super(Mgr, self).__init__(parser=SecOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

