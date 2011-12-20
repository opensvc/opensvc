#!/opt/opensvc/bin/python

"""
OSVC_COMP_ETCSYSTEM='[{"key": "fcp:fcp_offline_delay", "value": [">=", 21]}, {"key": "ssd:ssd_io_time", "value": ["=", "0x3C"]}]' ./etcsystem.py OSVC_COMP_ETCSYSTEM check
"""

import os
import sys
import json
import pwd
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class EtcSystem(object):
    def __init__(self, prefix='OSVC_COMP_FILELINE_'):
        self.prefix = prefix.upper()

        self.cf = os.path.join(os.sep, 'etc', 'system')
        self.keys = []
        self.load_file(self.cf)

        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.keys += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'key syntax error on var[', k, '] = ',os.environ[k]

        if len(self.keys) == 0:
            print >>sys.stderr, "no applicable variable found in rulesets", self.prefix
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
        self.lines = map(lambda x: ' '.join(x.split()), self.lines)

    def set_val(self, keyname, value):
        done = False
        for i, line in enumerate(self.lines):
            if line.startswith('*'):
                continue
            l = line.split()
            if len(l) != 2:
                continue
            if l[0] != 'set':
                continue
            v = l[1].split('=')
            if len(v) != 2:
                continue
            if keyname == v[0]:
                if done:
                    self.lines[i] = '* '+line
                else:
                    v[1] = str(value)
                    newline = 'set %s=%s'%(keyname, str(value))
                    print "modify '%s' in /etc/system"%newline
                    self.lines[i] = newline
                    done = True
        if not done:
            newline = 'set %s=%s'%(keyname, str(value))
            print "add '%s' to /etc/system"%newline
            self.lines.append(newline)

    def get_val(self, keyname):
        for line in self.lines:
            if line.startswith('*'):
                continue
            l = line.split()
            if len(l) != 2:
                continue
            if l[0] != 'set':
                continue
            v = l[1].split('=')
            if len(v) != 2:
                continue
            if keyname == v[0]:
                val = v[1]
                try:
                    val = int(val)
                except:
                    pass
                return val
        return None

    def check_key(self, key, verbose=True):
        if 'key' not in key:
            if verbose:
                print >>sys.stderr, "'key' not set in rule %s"%str(key)
            return RET_NA
        if 'value' not in key:
            if verbose:
                print >>sys.stderr, "'value' not set in rule %s"%str(key)
            return RET_NA
        if type(key['value']) != list:
            if verbose:
                print >>sys.stderr, "'value' is not a list: %s"%str(key)
            return RET_NA
        if len(key['value']) != 2:
            if verbose:
                print >>sys.stderr, "'value' list must have 2 members: %s"%str(key)
            return RET_NA
        op = key['value'][0]
        target = key['value'][1]
        if op not in ('>=', '<=', '='):
            if verbose:
                print >>sys.stderr, "'value' list member 0 must be either '=', '>=' or '<=': %s"%str(key)
            return RET_NA
        keyname = key['key']
        value = self.get_val(keyname)
        if value is None:
            if verbose:
                print >>sys.stderr, "%s not set"%keyname
            return RET_ERR
        if op == '=':
            if str(value) != str(target):
                if verbose:
                    print >>sys.stderr, "%s=%s, target: %s"%(keyname, str(value), str(target))
                return RET_ERR
        else:
            if type(value) != int:
                if verbose:
                    print >>sys.stderr, "%s=%s value must be integer"%(keyname, str(value))
                return RET_ERR
            elif op == '<=' and value > target:
                if verbose:
                    print >>sys.stderr, "%s=%s target: <= %s"%(keyname, str(value), str(target))
                return RET_ERR
            elif op == '>=' and value < target:
                if verbose:
                    print >>sys.stderr, "%s=%s target: >= %s"%(keyname, str(value), str(target))
                return RET_ERR
        if verbose:
            print "%s=%s on target"%(keyname, str(value))
        return RET_OK

    def fix_key(self, key):
        self.set_val(key['key'], key['value'][1])

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

