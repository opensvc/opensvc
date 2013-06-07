#!/opt/opensvc/bin/python

"""
OSVC_COMP_MPATH='[{"key": "defaults.polling_interval", "op": ">=", "value": 20}, {"key": "device.HP.HSV210.prio", "op": "=", "value": "alua"}]' ./linux.mpath.py OSVC_COMP_MPATH check
"""

import os
import sys
import json

sys.path.append(os.path.dirname(__file__))

from comp import *
from keyval_parser import Parser, ParserError

class KeyVal(object):
    def __init__(self, prefix='OSVC_COMP_KEYVAL_', path=None):
        self.prefix = prefix.upper()
        self.cf = path
        self.nocf = False
        if path is None:
            print >>sys.stderr, "no file path specified"
            raise NotApplicable()

        self.keys = []
        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.keys += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'key syntax error on var[', k, '] = ',os.environ[k]

        if len(self.keys) == 0:
            print "no applicable variable found in rulesets", self.prefix
            raise NotApplicable()

        self.conf = Parser(path)


    def fixable(self):
        return RET_OK

    def _check_key(self, keyname, target, op, value, verbose=True):
        r = RET_OK
        if op == "unset":
            if value is not None:
                if verbose:
                    print >>sys.stderr, "%s is set, should not be"%keyname
                return RET_ERR
            else:
                if verbose:
                    print "%s is not set, on target"%keyname
                return RET_OK

        if value is None:
            if verbose:
                print >>sys.stderr, "%s is not set, target: %s"%(keyname, str(target))
            return RET_ERR

        if type(value) == list:
            if str(target) in value:
                if verbose:
                    print "%s=%s on target"%(keyname, str(value))
                return RET_OK
            else:
                if verbose:
                    print >>sys.stderr, "%s=%s is not set"%(keyname, str(target))
                return RET_ERR

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

        if op not in ('>=', '<=', '=', 'unset'):
            if verbose:
                print >>sys.stderr, "'op' must be either '=', '>=' or '<=': %s"%str(key)
            return RET_NA

        keyname = key['key']
        value = self.conf.get(keyname)

        r = self._check_key(keyname, target, op, value, verbose=verbose)

        return r

    def fix_key(self, key):
        if key['op'] == "unset":
            print "%s unset"%key['key']
            self.conf.unset(key['key'])
        else:
            print "%s=%s set"%(key['key'], key['value'])
            self.conf.set(key['key'], key['value'])

    def check(self):
        r = 0
        for key in self.keys:
            r |= self.check_key(key, verbose=True)
        return r

    def fix(self):
        for key in self.keys:
            if self.check_key(key, verbose=False) == RET_ERR:
                self.fix_key(key)
        if not self.conf.changed:
            return
        try:
            self.conf.write()
        except ParserError as e:
            print >>sys.stderr, e
            return RET_ERR
        return RET_OK

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix configfile_path"""%sys.argv[0]
    if len(sys.argv) != 4:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = KeyVal(sys.argv[1], sys.argv[3])
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

