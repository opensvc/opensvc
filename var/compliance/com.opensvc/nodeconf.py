#!/opt/opensvc/bin/python

"""
OSVC_COMP_NODECONF='[{"key": "node.repopkg", "op": "=", "value": "ftp://servername/opensvc"}, {"key": "node.repocomp", "op": "=", "value": "ftp://servername/"}]' ./nodeconf.py OSVC_COMP_NODECONF check
"""

import os
import sys
import json
import re
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class NodeConf(object):
    def __init__(self, prefix='OSVC_COMP_NODECONF_'):
        self.prefix = prefix.upper()
        self.keys = []

        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.keys += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'key syntax error on var[', k, '] = ',os.environ[k]

        if len(self.keys) == 0:
            raise NotApplicable()

        for key in self.keys:
            if "value" in key:
                key['value'] = self.subst(key['value'])

    def subst(self, v):
        if type(v) == list:
            l = []
            for _v in v:
                l.append(self.subst(_v))
            return l
        if type(v) != str and type(v) != unicode:
            return v

        p = re.compile('%%ENV:\w+%%')
        for m in p.findall(v):
            s = m.strip("%").replace('ENV:', '')
            if s in os.environ:
                _v = os.environ[s]
            elif 'OSVC_COMP_'+s in os.environ:
                _v = os.environ['OSVC_COMP_'+s]
            else:
                print >>sys.stderr, s, 'is not an env variable'
                raise NotApplicable()
            v = v.replace(m, _v)
        return v

    def fixable(self):
        return RET_OK

    def set_val(self, keyname, target):
        if type(target) == int:
            target = str(target)
        cmd = ['/opt/opensvc/bin/nodemgr', 'set', '--param', keyname, '--value', target]
        print ' '.join(cmd)
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        return p.returncode

    def get_val(self, keyname):
        cmd = ['/opt/opensvc/bin/nodemgr', 'get', '--param', keyname]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            #print >>sys.stderr, '\n'.join((' '.join(cmd), out, err))
            return
        out = out.strip()
        try:
            out = int(out)
        except:
            pass
        return out

    def _check_key(self, keyname, target, op, value, verbose=True):
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
        value = self.get_val(keyname)

        if value is None:
            print >>sys.stderr, "%s key is not set"%keyname
            return RET_ERR

        return self._check_key(keyname, target, op, value, verbose)

    def fix_key(self, key):
        return self.set_val(key['key'], key['value'])

    def check(self):
        r = 0
        for key in self.keys:
            r |= self.check_key(key, verbose=True)
        return r

    def fix(self):
        r = 0
        for key in self.keys:
            if self.check_key(key, verbose=False) == RET_ERR:
                r += self.fix_key(key)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = NodeConf(sys.argv[1])
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

