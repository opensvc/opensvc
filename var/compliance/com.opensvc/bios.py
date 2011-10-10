#!/opt/opensvc/bin/python

import os
import sys
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompBios(object):
    def __init__(self, target):
        self.target = target
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname not in ['Linux']:
            print >>sys.stderr, 'module not supported on', self.sysname
            raise NotApplicable()

    def get_bios_version_Linux(self):
        p = os.path.join(os.sep, 'sys', 'class', 'dmi', 'id', 'bios_version')
        try:
            f = open(p, 'r')
            ver = f.read().strip()
            f.close()
        except:
            print >>sys.stderr, 'can not fetch bios version from %s'%p
            return None
        return ver

    def fixable(self):
        return RET_NA

    def check(self):
        ver = self.get_bios_version_Linux()
        if ver is None:
            return RET_NA
        if ver == self.target:
            print "bios version is %s, on target"%ver
            return RET_OK
        print >>sys.stderr, "bios version is %s, target %s"%(ver, self.target)
        return RET_ERR

    def fix(self):
        return RET_NA

if __name__ == "__main__":
    syntax = """syntax:
      %s TARGET check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = CompBios(sys.argv[1])
        if sys.argv[2] == 'check':
            RET = o.check()
        elif sys.argv[2] == 'fix':
            RET = o.fix()
        elif sys.argv[2] == 'fixable':
            RET = o.fixable()
        else:
            print >>sys.stderr, "unsupported argument '%s'"%sys.argv[2]
            print >>sys.stderr, syntax
            RET = RET_ERR
    except NotApplicable:
        sys.exit(RET_NA)
    except:
        import traceback
        traceback.print_exc()
        sys.exit(RET_ERR)

    sys.exit(RET)

