#!/opt/opensvc/bin/python

"""
OSVC_COMP_ETCSYSTEM='[{"key": "fcp:fcp_offline_delay", "op": ">=", "value": 21}, {"key": "ssd:ssd_io_time", "op": "=", "value": "0x3C"}]' ./etcsystem.py OSVC_COMP_ETCSYSTEM check
"""

import os
import sys
import json
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class EtcSystem(object):
    def __init__(self, prefix='OSVC_COMP_FILELINE_'):
        self.prefix = prefix.upper()
        self.data = {}

        self.cf = os.path.join(os.sep, 'etc', 'system')
        self.keys = []
        self.load_file(self.cf)

        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.keys += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'key syntax error on var[', k, '] = ',os.environ[k]

        if len(self.keys) == 0:
            raise NotApplicable()

    def fixable(self):
        return RET_OK

    def load_file(self, p):
        if not os.path.exists(p):
            print >>sys.stderr, p, "does not exist"
            return
        with open(p, 'r') as f:
            buff = f.read()
        self.lines = buff.split('\n')
        for i, line in enumerate(self.lines):
            line = line.strip()
            if line.startswith('*'):
                continue
            if len(line) == 0:
                continue
            l = line.split()
            if l[0] != "set":
                continue
            if len(l) < 2:
                continue
            line = ' '.join(l[1:]).split('*')[0]
            var, val = line.split('=')
            var = var.strip()
            val = val.strip()
            try:
                val = int(val)
            except:
                pass
            if var in self.data:
                self.data[var].append([val, i])
            else:
                self.data[var] = [[val, i]]

    def set_val(self, keyname, target, op):
        newline = 'set %s = %s'%(keyname, str(target))
        if keyname not in self.data:
            print "add '%s' to /etc/system"%newline
            self.lines.insert(-1, newline + " * added by opensvc")
        else:
            ok = 0
            for value, ref in self.data[keyname]:
                r = self._check_key(keyname, target, op, value, ref, verbose=False)
                if r == RET_ERR:
                    print "comment out line %d: %s"%(ref, self.lines[ref])
                    self.lines[ref] = '* '+self.lines[ref]+' * commented out by opensvc'
                else:
                    ok += 1
            if ok == 0:
                print "add '%s' to /etc/system"%newline
                self.lines.insert(-1, newline + " * added by opensvc")

    def get_val(self, keyname):
        if keyname not in self.data:
            return []
        return self.data[keyname]

    def _check_key(self, keyname, target, op, value, ref, verbose=True):
        r = RET_OK
        if value is None:
            if verbose:
                print >>sys.stderr, "%s not set"%keyname
            r |= RET_ERR
        if op == '=':
            if str(value) != str(target):
                if verbose:
                    print >>sys.stderr, "%s=%s, target: %s"%(keyname, str(value), str(target))
                r |= RET_ERR
            elif verbose:
                print "%s=%s on target"%(keyname, str(value))
        else:
            if type(value) != int:
                if verbose:
                    print >>sys.stderr, "%s=%s value must be integer"%(keyname, str(value))
                r |= RET_ERR
            elif op == '<=' and value > target:
                if verbose:
                    print >>sys.stderr, "%s=%s target: <= %s"%(keyname, str(value), str(target))
                r |= RET_ERR
            elif op == '>=' and value < target:
                if verbose:
                    print >>sys.stderr, "%s=%s target: >= %s"%(keyname, str(value), str(target))
                r |= RET_ERR
            elif verbose:
                print "%s=%s on target"%(keyname, str(value))
        return r

    def check_key(self, key, verbose=True):
        if 'key' not in key:
            if verbose:
                print >>sys.stderr, "'key' not set in rule %s"%str(key)
            return RET_NA
        if 'value' not in key:
            if verbose:
                print >>sys.stderr, "'value' not set in rule %s"%str(key)
            return RET_NA
        if 'op' not in key:
            op = "="
        else:
            op = key['op']
        target = key['value']

        if op not in ('>=', '<=', '='):
            if verbose:
                print >>sys.stderr, "'value' list member 0 must be either '=', '>=' or '<=': %s"%str(key)
            return RET_NA

        keyname = key['key']
        data = self.get_val(keyname)

        if len(data) == 0:
            print >>sys.stderr, "%s key is not set"%keyname
            return RET_ERR

        r = RET_OK
        ok = 0
        for value, ref in data:
            r |= self._check_key(keyname, target, op, value, ref, verbose)
            if r == RET_OK:
                ok += 1

        if ok > 1:
            print >>sys.stderr, "duplicate lines for key %s"%keyname
            r |= RET_ERR
        return r

    def fix_key(self, key):
        self.set_val(key['key'], key['value'], key['op'])

    def check(self):
        r = 0
        for key in self.keys:
            r |= self.check_key(key, verbose=True)
        return r

    def fix(self):
        for key in self.keys:
            if self.check_key(key, verbose=False) == RET_ERR:
                self.fix_key(key)
        if len(self.keys) > 0:
            import datetime
            backup = self.cf+str(datetime.datetime.now())
            try:
                import shutil
                shutil.copy(self.cf, backup)
            except:
                print >>sys.stderr, "failed to backup %s"%self.cf
                return RET_ERR
            try:
                with open(self.cf, 'w') as f:
                    f.write('\n'.join(self.lines))
            except:
                print >>sys.stderr, "failed to write %s"%self.cf
                return RET_ERR
        return RET_OK

if __name__ == "__main__":
    """ test: OSVC_COMP_SYSCTL='[{"key": "net.unix.max_dgram_qlen", "value": [">=", 9]}, {"key": "kernel.ctrl-alt-del", "value": ["=", 1]}, {"key": "kernel.printk", "value": [[], [] , [], [">=", 12]]}]' ./sysctl.py OSVC_COMP_SYSCTL check
    """
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = EtcSystem(sys.argv[1])
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

