#!/usr/bin/env /opt/opensvc/bin/python
""" 
module use OSVC_COMP_USER_... vars
which define {"gssftp": {"disable": "no", "server_args": "-l -a -u 022", ...}, ...}

supported dictionnary keys:
- disable
- server_args
"""

import os
import sys
import json
import pwd
from subprocess import Popen, list2cmdline

sys.path.append(os.path.dirname(__file__))

from comp import *

class Xinetd(object):
    def __init__(self, prefix='OSVC_COMP_XINETD_'):
        self.prefix = prefix.upper()
        self.base = os.path.join(os.sep, "etc", "xinetd.d")
        if not os.path.exists(self.base):
            print >>sys.stderr, self.base, 'does not exist'
            raise NotApplicable()

        self.svcs = {}
        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.svcs.update(json.loads(os.environ[k]))
            except ValueError:
                print >>sys.stderr, 'xinetd service syntax error on var[', k, '] = ',os.environ[k]

        if len(self.svcs) == 0:
            raise NotApplicable()
        self.cf_d = {}
        self.known_props = (
            "flags",
            "socket_type",
            "wait",
            "user",
            "server",
            "server_args",
            "disable")

    def fixable(self):
        return RET_NA

    def get_svc(self, svc):
        if svc in self.cf_d:
            return self.cf_d[svc]

        p = os.path.join(self.base, svc)
        if not os.path.exists(p):
            self.cf_d[svc] = {}
            return self.cf_d[svc]

        if svc not in self.cf_d:
            self.cf_d[svc] = {}

        with open(p, 'r') as f:
            for line in f.read().split('\n'):
                if '=' not in line:
                    continue
                l = line.split('=')
                if len(l) != 2:
                    continue
                var = l[0].strip()
                val = l[1].strip()
                self.cf_d[svc][var] = val

        return self.cf_d[svc]

    def fix_item(self, svc, item, target):
        if item not in self.known_props:
            print >>sys.stderr, 'xinetd service', svc, item+': unknown property in compliance rule'
            return RET_ERR
        cf = self.get_svc(svc)

        if item in cf and cf[item] == target:
            return RET_OK

        p = os.path.join(self.base, svc)
        if not os.path.exists(p):
            print >>sys.stderr, p, "does not exist"
            return RET_ERR

        done = False
        with open(p, 'r') as f:
            buff = f.read().split('\n')
            for i, line in enumerate(buff):
                if '=' not in line:
                    continue
                l = line.split('=')
                if len(l) != 2:
                    continue
                var = l[0].strip()
                if var != item:
                    continue
                l[1] = target
                buff[i] = "= ".join(l)
                done = True

        if not done:
            with open(p, 'r') as f:
                buff = f.read().split('\n')
                for i, line in enumerate(buff):
                    if '=' not in line:
                        continue
                    l = line.split('=')
                    if len(l) != 2:
                        continue
                    buff.insert(i, item+" = "+target)
                    done = True
                    break

        if not done:
            print >>sys.stderr, "failed to set", item, "=", target, "in", p
            return RET_ERR

        with open(p, 'w') as f:
            f.write("\n".join(buff))

        print "set", item, "=", target, "in", p
        return RET_OK

    def check_item(self, svc, item, target, verbose=False):
        if item not in self.known_props:
            print >>sys.stderr, 'xinetd service', svc, item+': unknown property in compliance rule'
            return RET_ERR
        cf = self.get_svc(svc)
        if item in cf and target == cf[item]:
            if verbose:
                print 'xinetd service', svc, item+':', cf[item]
            return RET_OK
        elif item in cf:
            if verbose:
                print >>sys.stderr, 'xinetd service', svc, item+':', cf[item], 'target:', target
        else:
            if verbose:
                print >>sys.stderr, 'xinetd service', svc, item+': unset', 'target:', target
        return RET_ERR

    def check_svc(self, svc, props):
        r = 0
        for prop in props:
            r |= self.check_item(svc, prop, props[prop], verbose=True)
        return r

    def fix_svc(self, svc, props):
        r = 0
        for prop in props:
            r |= self.fix_item(svc, prop, props[prop])
        return r

    def check(self):
        r = 0
        for svc, props in self.svcs.items():
            r |= self.check_svc(svc, props)
        return r

    def fix(self):
        r = 0
        for svc, props in self.svcs.items():
            r |= self.fix_svc(svc, props)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = Xinetd(sys.argv[1])
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

