#!/usr/bin/env /opt/opensvc/bin/python

"""
OSVC_COMP_SERVICES_SVCNAME=app2.prd OSVC_COMP_SVCCONF_APP2_PRD='[{"value": "fd5373b3d938", "key": "container#1.run_image", "op": "="}, {"value": "/bin/sh", "key": "container#1.run_command", "op": "="}, {"value": "/opt/%%ENV:SERVICES_SVCNAME%%", "key": "DEFAULT.docker_data_dir", "op": "="}, {"value": "no", "key": "container(type=docker).disable", "op": "="}, {"value": 123, "key": "container(type=docker&&run_command=/bin/sh).newvar", "op": "="}]' ./svcconf.py OSVC_COMP_SVCCONF check
"""

import os
import sys
import json
import re
import copy
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class SvcConf(object):
    def __init__(self, prefix='OSVC_COMP_SVCCONF_'):
        self.prefix = prefix.upper()
        self.keys = []

        if "OSVC_COMP_SERVICES_SVCNAME" not in os.environ:
            print "SERVICES_SVCNAME is not set"
            raise NotApplicable()

        self.svcname = os.environ['OSVC_COMP_SERVICES_SVCNAME']

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

        try:
            self.get_env_file(refresh=True)
        except Exception as e:
            print >>sys.stderr, "unable to load service configuration: %s", str(e)
            raise ComplianceError()

        self.sanitize_keys()
        self.expand_keys()

    def get_env_file(self, refresh=False):
       if not refresh:
           return self.svcenv
       cmd = ['/opt/opensvc/bin/svcmgr', '-s', self.svcname, 'json_env']
       p = Popen(cmd, stdout=PIPE, stderr=PIPE)
       out, err = p.communicate()
       for line in out.split('\n'):
           if line.startswith('{'):
               out = line
               break
       self.svcenv = json.loads(out)
       return self.svcenv

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
        return RET_NA

    def set_val(self, keyname, target):
        if type(target) == int:
            target = str(target)
        cmd = ['/opt/opensvc/bin/svcmgr', '-s', self.svcname, 'set', '--param', keyname, '--value', target]
        print ' '.join(cmd)
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        return p.returncode

    def get_val(self, keyname):
        section, var = keyname.split('.')
        if section not in self.svcenv:
            return None
        return self.svcenv[section].get(var)

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

    def check_filter(self, section, filter):
        op = None
        i = 0
        try:
            i = filter.index("&&")
            op = "and"
        except ValueError:
            pass
        try:
            i = filter.index("||")
            op = "or"
        except ValueError:
            pass

        if i == 0:
            _filter = filter
            _tail = ""
        else:
            _filter = filter[:i]
            _tail = filter[i:].lstrip("&&").lstrip("||")

        r = self._check_filter(section, _filter)
        #print " _check_filter('%s', '%s') => %s" % (section, _filter, str(r))

        if op == "and":
            r &= self.check_filter(section, _tail)
        elif op == "or":
            r |= self.check_filter(section, _tail)

        return r

    def _check_filter(self, section, filter):
        if "~=" in filter:
            return self._check_filter_reg(section, filter)
        elif "=" in filter:
            return self._check_filter_eq(section, filter)
        print >>sys.stderr, "invalid filter syntax: %s" % filter
        return False

    def _check_filter_eq(self, section, filter):
        l = filter.split("=")
        if len(l) != 2:
            print >>sys.stderr, "invalid filter syntax: %s" % filter
            return False
        key, val = l
        cur_val = self.svcenv[section].get(key)
        if cur_val is None:
            return False
        if str(cur_val) == str(val):
            return True
        return False

    def _check_filter_reg(self, section, filter):
        l = filter.split("~=")
        if len(l) != 2:
            print >>sys.stderr, "invalid filter syntax: %s" % filter
            return False
        key, val = l
        val = val.strip("/")
        cur_val = self.svcenv[section].get(key)
        if cur_val is None:
            return False
        reg = re.compile(val)
        if reg.match(cur_val):
            return True
        return False

    def resolve_sections(self, s, filter):
        """
        s is a ressource section name (fs, container, app, sync, ...)
        filter is a regexp like expression
           container(type=docker)
           fs(mnt~=/.*tools/)
           container(type=docker&&run_image~=/opensvc\/collector_web:build.*/)
           fs(mnt~=/.*tools/||mnt~=/.*moteurs/)
        """
        result = [];
        eligiblesections = [];
        for section in self.svcenv.keys():
            if section.startswith(s+'#') or section == s:
                eligiblesections.append(section)
        for section in eligiblesections:
            if self.check_filter(section, filter):
                #print "   =>", section, "matches filter"
                result.append(section)
        result.sort()
        return result

    def sanitize_keys(self, verbose=True):
        r = RET_OK
        for key in self.keys:
            if 'key' not in key:
                if verbose:
                    print >>sys.stderr, "'key' not set in rule %s"%str(key)
                r |= RET_NA
            if 'value' not in key:
                if verbose:
                    print >>sys.stderr, "'value' not set in rule %s"%str(key)
                r |= RET_NA
            if 'op' not in key:
                op = "="
            else:
                op = key['op']

            if op not in ('>=', '<=', '='):
                if verbose:
                    print >>sys.stderr, "'value' list member 0 must be either '=', '>=' or '<=': %s"%str(key)
                r |= RET_NA

        if r is not RET_OK:
            sys.exit(r)

    def expand_keys(self):
        expanded_keys = []

        for key in self.keys:
            keyname = key['key']
            target = key['value']
            op = key['op']
            sectionlist = [];
            reg1 = re.compile(r'(.*)\((.*)\)\.(.*)')
            reg2 = re.compile(r'(.*)\.(.*)')
            m = reg1.search(keyname)
            if m:
                section = m.group(1)
                filter = m.group(2)
                var = m.group(3)
                sectionlist = self.resolve_sections(section, filter)
                for resolvedsection in sectionlist:
                    newdict = {
                     'key': '.'.join([resolvedsection, var]),
                     'op': op,
                     'value': target
                    }
                    expanded_keys.append(newdict)
                continue
            m = reg2.search(keyname)
            if m:
                section = m.group(1)
                var = m.group(2)
                expanded_keys.append(copy.copy(key))
                continue

            # drop key

        self.keys = expanded_keys

    def check_key(self, key, verbose=True):
        op = key['op']
        target = key['value']
        keyname = key['key']

        value = self.get_val(keyname)

        if value is None:
            if verbose:
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
        o = SvcConf(sys.argv[1])
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

