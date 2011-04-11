#!/opt/opensvc/bin/python
""" 
module use OSVC_COMP_GROUP_... vars
which define {'groupname':{'propname':'propval',... }, ...}

example: 
{"tibco":{"gid":1000,"members":"tibco,tibadm",},
 "tibco1":{"gid":1001,"members":"tibco",},
}

supported dictionnary keys:
- members

dictionnary keys used by another module:
- gid
"""

import os
import sys
import json
import grp
from subprocess import Popen

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompGroupMembership(object):
    def __init__(self, prefix='OSVC_COMP_GROUP_'):
        self.prefix = prefix
        self.grt = {
            'members': 'gr_mem',
        }
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname not in ['SunOS', 'Linux']:
            print >>sys.stderr, 'module not supported on', self.sysname
            raise NotApplicable

        #
        # initialize a hash to store all group membership
        # of users
        #
        self.member_of = {}
        for group in grp.getgrall():
            for user in group.gr_mem:
                if user in self.member_of:
                    self.member_of[user].append(group.gr_name)
                else:
                    self.member_of[user] = [group.gr_name]

        self.groups = {}
        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.groups.update(json.loads(os.environ[k]))
            except ValueError:
                print 'group syntax error on var[', k, '] = ', os.environ[k]

        if len(self.groups) == 0:
            print "no applicable variable found in rulesets", self.prefix
            raise NotApplicable

    def fixable(self):
        return RET_NA

    def del_member(self, group, user):
        if user not in self.member_of:
            return 0
        g = set(self.member_of[user]) - set([group])
        g = ','.join(g)
        return self.fix_member(g, user)

    def add_member(self, group, user):
        if user in self.member_of:
            g = set(self.member_of[user]) | set([group])
            g = ','.join(g)
        else:
            g = group
        return self.fix_member(g, user)

    def fix_member(self, g, user):
        cmd = ['usermod', '-G', g, user]
        print ' '.join(cmd)
        p = Popen(cmd)
        out, err = p.communicate()
        r = p.returncode
        if r == 0:
            return RET_OK
        else:
            return RET_ERR

    def fix_members(self, group, target):
        r = 0
        for user in target:
            if user in self.member_of and group in self.member_of[user]:
                continue
            r += self.add_member(group, user)
        for user in [u for u in self.member_of if group in self.member_of[u] and u not in target]:
            r += self.del_member(group, user)
        return r

    def fix_item(self, group, item, target):
        if item == 'members':
            return self.fix_members(group, target)
        else:
            print >>sys.stderr, 'no fix implemented for', item
            return RET_ERR

    def check_item(self, group, item, target, current, verbose=False):
        if (isinstance(current, list) and set(target) == set(current)) or target == current:
            if verbose:
                print 'OK: group:', group, item+':', current
            return RET_OK
        else:
            if verbose:
                print >>sys.stderr, 'group:', group, item+':', current, 'target:', target
            return RET_ERR

    def check_group(self, group, props):
        r = 0
        try:
            groupinfo = grp.getgrnam(group)
        except KeyError:
            print 'group', group, 'does not exist'
            return RET_OK
        for prop in self.grt:
            if prop in props:
                r |= self.check_item(group, prop, props[prop], getattr(groupinfo, self.grt[prop]), verbose=True)
        return r

    def fix_group(self, group, props):
        r = 0
        try:
            groupinfo = grp.getgrnam(group)
        except KeyError:
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

    def fix(self):
        r = 0
        for group, props in self.groups.items():
            r |= self.fix_group(group, props)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    o = CompGroupMembership(sys.argv[1])
    try:
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

