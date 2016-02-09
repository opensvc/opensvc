#!/usr/bin/env /opt/opensvc/bin/python

"""
OSVC_COMP_SSH='[{"key": "PermitRootLogin", "op": "=", "value": "yes"}]' /opt/opensvc/bin/python /opt/opensvc/var/compliance/com.opensvc/keyval.py OSVC_COMP_SSH check /etc/ssh/sshd_config
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
            raise NotApplicable()

        self.target_n_key = {}
        for i, key in enumerate(self.keys):
             if self.keys[i]['op'] == 'IN':
                 self.keys[i]['value'] = json.loads(self.keys[i]['value'])
             if 'op' in key and 'key' in key and key['op'] not in ("unset", "reset"):
                 if key['key'] not in self.target_n_key:
                     self.target_n_key[key['key']] = 1
                 else:
                     self.target_n_key[key['key']] += 1
        try:
            self.conf = Parser(path)
        except ParserError as e:
            print >>sys.stderr, e
            raise ComplianceError()


    def fixable(self):
        return RET_OK

    def _check_key(self, keyname, target, op, value, instance=0, verbose=True):
        r = RET_OK
        if op == "reset":
            if value is not None:
                current_n_key = len(value)
                target_n_key = self.target_n_key[keyname] if keyname in self.target_n_key else 0
                if current_n_key > target_n_key:
                    if verbose:
                        print >>sys.stderr, "%s is set %d times, should be set %d times"%(keyname, current_n_key, target_n_key)
                    return RET_ERR
                else:
                    if verbose:
                        print "%s is set %d times, on target"%(keyname, current_n_key)
                    return RET_OK
            else:
                return RET_OK
        elif op == "unset":
            if value is not None:
                if target.strip() == "":
                    if verbose:
                        print >>sys.stderr, "%s is set, should not be"%keyname
                    return RET_ERR
                target_found = False
                for i, val in enumerate(value):
                    if target == val:
                        target_found = True
                        break

                if target_found:
                    if verbose:
                        print >>sys.stderr, "%s[%d] is set to value %s, should not be"%(keyname, i, target)
                    return RET_ERR
                else:
                    if verbose:
                        print "%s is not set to value %s, on target"%(keyname, target)
                    return RET_OK
            else:
                if target.strip() != "":
                    if verbose:
                        print "%s=%s is not set, on target"%(keyname, target)
                else:
                    if verbose:
                        print "%s is not set, on target"%keyname
                return RET_OK

        if value is None:
            if op == 'IN' and "unset" in map(str, target):
                if verbose:
                    print "%s is not set, on target"%(keyname)
                return RET_OK
            else:
                if verbose:
                    print >>sys.stderr, "%s[%d] is not set, target: %s"%(keyname, instance, str(target))
                return RET_ERR

        if type(value) == list:
            if str(target) in value:
                if verbose:
                    print "%s[%d]=%s on target"%(keyname, instance, str(value))
                return RET_OK
            else:
                if verbose:
                    print >>sys.stderr, "%s[%d]=%s is not set"%(keyname, instance, str(target))
                return RET_ERR

        if op == '=':
            if str(value) != str(target):
                if verbose:
                    print >>sys.stderr, "%s[%d]=%s, target: %s"%(keyname, instance, str(value), str(target))
                r |= RET_ERR
            elif verbose:
                print "%s=%s on target"%(keyname, str(value))
        elif op == 'IN':
            if str(value) not in map(str, target):
                if verbose:
                    print >>sys.stderr, "%s[%d]=%s, target: %s"%(keyname, instance, str(value), str(target))
                r |= RET_ERR
            elif verbose:
                print "%s=%s on target"%(keyname, str(value))
        else:
            if type(value) != int:
                if verbose:
                    print >>sys.stderr, "%s[%d]=%s value must be integer"%(keyname, instance, str(value))
                r |= RET_ERR
            elif op == '<=' and value > target:
                if verbose:
                    print >>sys.stderr, "%s[%d]=%s target: <= %s"%(keyname, instance, str(value), str(target))
                r |= RET_ERR
            elif op == '>=' and value < target:
                if verbose:
                    print >>sys.stderr, "%s[%d]=%s target: >= %s"%(keyname, instance, str(value), str(target))
                r |= RET_ERR
            elif verbose:
                print "%s[%d]=%s on target"%(keyname, instance, str(value))
        return r

    def check_key(self, key, instance=0, verbose=True):
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

        allowed_ops = ('>=', '<=', '=', 'unset', 'reset', 'IN')
        if op not in allowed_ops:
            if verbose:
                print >>sys.stderr, key['key'], "'op' value must be one of", ", ".join(allowed_ops)
            return RET_NA

        keyname = key['key']
        value = self.conf.get(keyname, instance=instance)

        r = self._check_key(keyname, target, op, value, instance=instance, verbose=verbose)

        return r

    def fix_key(self, key, instance=0):
        if key['op'] == "unset" or (key['op'] == "IN" and key['value'][0] == "unset"):
            print "%s unset"%key['key']
            if key['op'] == "IN":
                target = None
            else:
                target = key['value']
            self.conf.unset(key['key'], target)
        elif key['op'] == "reset":
            target_n_key = self.target_n_key[key['key']] if key['key'] in self.target_n_key else 0
            print "%s truncated to %d definitions"%(key['key'], target_n_key)
            self.conf.truncate(key['key'], target_n_key)
        else:
            if key['op'] == "IN":
                target = key['value'][0]
            else:
                target = key['value']
            print "%s=%s set"%(key['key'], target)
            self.conf.set(key['key'], target, instance=instance)

    def check(self):
        r = 0
        key_instance = {}
        for key in self.keys:
            if 'key' not in key or 'op' not in key:
                continue
            if key['op'] in ('reset', 'unset'):
                instance = None
            else:
                if key['key'] not in key_instance:
                    key_instance[key['key']] = 0
                else:
                    key_instance[key['key']] += 1
                instance = key_instance[key['key']]
            r |= self.check_key(key, instance=instance, verbose=True)
        return r

    def fix(self):
        key_instance = {}
        for key in self.keys:
            if 'key' not in key or 'op' not in key:
                continue
            if key['op'] in ('reset', 'unset'):
                instance = None
            else:
                if key['key'] not in key_instance:
                    key_instance[key['key']] = 0
                else:
                    key_instance[key['key']] += 1
                instance = key_instance[key['key']]
            if self.check_key(key, instance=instance, verbose=False) == RET_ERR:
                self.fix_key(key, instance=instance)
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
    except ComplianceError:
        sys.exit(RET_ERR)
    except NotApplicable:
        sys.exit(RET_NA)
    except:
        import traceback
        traceback.print_exc()
        sys.exit(RET_ERR)

    sys.exit(RET)

