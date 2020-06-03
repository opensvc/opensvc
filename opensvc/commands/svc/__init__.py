# coding: utf8

import sys
from commands.mgr import Mgr as BaseMgr
from commands.svc.parser import SvcOptParser

class Mgr(BaseMgr):
    def __init__(self, node=None):
        super(Mgr, self).__init__(parser=SvcOptParser, node=node)

if __name__ == "__main__":
    sys.exit(Mgr()())
