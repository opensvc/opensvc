# coding: utf8

import sys
from commands.vol.parser import VolOptParser
from commands.mgr import Mgr as BaseMgr

class Mgr(BaseMgr):
    def __init__(self, node=None):
        super(Mgr, self).__init__(parser=VolOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())

