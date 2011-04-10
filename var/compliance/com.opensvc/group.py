#!/opt/opensvc/bin/python
""" 
module use OSVC_COMP_GROUP_... vars
which define {'groupname':{'propname':'propval',... }, ...}

example: 
{
 "tibco":{"gid":1000,"members":"tibco,tibadm",},
 "tibco1":{"gid":1001,"members":"tibco",},
}

supported dictionnary keys:
- gid

dictionnary keys used by another module:
- members
"""

import os
import sys
import json
import grp
from subprocess import Popen 

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompGroup(object):
    def __init__(self, prefix='OSVC_COMP_GROUP_'):
        self.prefix = prefix
        self.grt = {
            'gid': 'gr_gid',
        }

        self.groupmod_p = {
            'gid': '-g',
        }

        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname not in ['SunOS', 'Linux']:
            print >>sys.stderr, 'module not supported on', self.sysname
            raise NotApplicable

        self.groups = {}
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.groups.update(json.loads(os.environ[k]))
            except ValueError:
                print >>sys.stderr, 'group syntax error on var[', k, '] = ', os.environ[k]

        if len(self.groups) == 0:
            print >>sys.stderr, "no applicable variable found in rulesets", self.prefix
            raise NotApplicable

    def fixable(self):
        return RET_NA

    def fix_item(self, group, item, target):
        if item in self.groupmod_p:
            cmd = ['groupmod', self.groupmod_p[item], str(target), group]
            print ' '.join(cmd)
            p = Popen(cmd)
            out, err = p.communicate()
            r = p.returncode
            if r == 0:
                return RET_OK
            else:
                return RET_ERR
        else:
            print >>sys.stderr, 'no fix implemented for', item
            return RET_ERR

    def check_item(self, group, item, target, current, verbose=False):
        if target == current:
            if verbose:
                print 'OK: group:', group, item+':', current
            return RET_OK
        else:
            if verbose:
                print >>sys.stderr, 'group:', group, item+':', current, 'target:', target
            return RET_ERR 

    def try_create_group(self, props):
        #
        # don't try to create group if passwd db is not 'files'
        # beware: 'files' db is the implicit default
        #
        if 'db' in props and props['db'] != 'files':
            return False
        if set(self.grt.keys()) <= set(props.keys()):
            return True
        return False

    def check_group(self, group, props):
        r = 0
        try:
            groupinfo = grp.getgrnam(group)
        except KeyError:
            if self.try_create_group(props):
                print >>sys.stderr, 'group', group, 'does not exist'
                return RET_ERR
            else:
                print 'group', group, 'does not exist and not enough info to create it'
                return RET_OK
        for prop in self.grt:
            if prop in props:
                r |= self.check_item(group, prop, props[prop], getattr(groupinfo, self.grt[prop]), verbose=True)
        return r

    def create_group(self, group, props):
        cmd = ['groupadd']
        for item in self.grt:
            cmd += [self.groupmod_p[item], str(props[item])]
        cmd += [group]
        print ' '.join(cmd)
        p = Popen(cmd)
        out, err = p.communicate()
        r = p.returncode
        if r == 0:
            return RET_OK
        else:
            return RET_ERR

    def fix_group(self, group, props):
        r = 0
        try:
            groupinfo = grp.getgrnam(group)
        except KeyError:
            if self.try_create_group(props):
                return self.create_group(group, props)
            else:
                print 'group', group, 'does not exist'
                return RET_OK
        for prop in self.grt:
            if prop in props and \
               self.check_item(group, prop, props[prop], getattr(groupinfo, self.grt[prop])) != RET_OK:
                r |= self.fix_item(group, prop, props[prop])
        return r

    def check(self):
        r = 0
        for group, props in self.groups.items():
            r |= self.check_group(group, props)
        return r

    def fix():
        r = 0
        for group, props in self.groups.items():
            r |= self.fix_group(group, props)
        return r

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >>sys.stderr, "need argument"
        sys.exit(RET_ERR)
    o = CompGroup()
    try:
        if sys.argv[1] == 'check':
            RET = o.check()
        elif sys.argv[1] == 'fix':
            RET = o.fix()
        elif sys.argv[1] == 'fixable':
            RET = o.fixable()
        else:
            print >>sys.stderr, "unsupported argument '%s'"%sys.argv[1]
            RET = RET_ERR
    except:
        import traceback
        traceback.print_exc()
        sys.exit(RET_ERR)

    sys.exit(RET)
