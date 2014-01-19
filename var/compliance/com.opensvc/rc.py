#!/opt/opensvc/bin/python
""" 
[{"service": "foo", "level": "2345", "state": "on"},
 {"service": "foo", "level": "016", "state": "off"},
 {"service": "bar", "state": "on"},
 ...]
"""

import os
import sys
import json
import pwd
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompRc(object):
    def __init__(self, prefix='OSVC_COMP_RC_'):
        self.prefix = prefix.upper()
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        self.services = []
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.services += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'failed to concatenate', os.environ[k], 'to service list'

        self.validate_svcs()

        if len(self.services) == 0:
            raise NotApplicable()

        if self.sysname not in ['Linux', 'HP-UX']:
            print >>sys.stderr, __file__, 'module not supported on', self.sysname
            raise NotApplicable()

        vendor = os.environ.get('OSVC_COMP_NODES_OS_VENDOR', 'unknown')
        release = os.environ.get('OSVC_COMP_NODES_OS_RELEASE', 'unknown')
        if vendor in ['CentOS', 'Redhat', 'Red Hat'] or \
           (vendor == 'Oracle' and self.sysname == 'Linux'):

            import chkconfig
            self.o = chkconfig.Chkconfig()
        elif vendor in ['HP']:
            import sysvinit
            self.o = sysvinit.SysVInit()
        else:
            print >>sys.stderr, vendor, "not supported"
            raise NotApplicable()

    def validate_svcs(self):
        l = []
        for i, svc in enumerate(self.services):
            if self.validate_svc(svc) == RET_OK:
                l.append(svc)
        self.svcs = l

    def validate_svc(self, svc):
        if 'service' not in svc:
            print >>sys.stderr, svc, ' rule is malformed ... service key not present'
            return RET_ERR
        if 'state' not in svc:
            print >>sys.stderr, svc, ' rule is malformed ... state key not present'
            return RET_ERR
        return RET_OK

    def check_svc(self, svc, verbose=True):
        if 'seq' in svc:
            seq = svc['seq']
        else:
            seq = None
        return self.o.check_state(svc['service'], svc['level'], svc['state'], seq=seq, verbose=verbose)

    def fix_svc(self, svc, verbose=True):
        if 'seq' in svc:
            seq = svc['seq']
        else:
            seq = None
        if self.check_svc(svc, verbose=False) == RET_OK:
            return RET_OK
        return self.o.fix_state(svc['service'], svc['level'], svc['state'], seq=seq)

    def check(self):
        r = 0
        for svc in self.services:
            r |= self.check_svc(svc)
        return r

    def fix(self):
        r = 0
        for svc in self.services:
            r |= self.fix_svc(svc)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = CompRc(sys.argv[1])
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

