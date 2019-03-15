# coding: utf8

import sys
from svcmgr_parser import SvcmgrOptParser
from mgr import Mgr

if __name__ == "__main__":
    ret = Mgr(parser=SvcmgrOptParser)()
    sys.exit(ret)
