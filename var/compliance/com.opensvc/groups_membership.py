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
from subprocess import *
from utilities import which

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompGroupMembership(object):
    def __init__(self, prefix='OSVC_COMP_GROUP_'):
        self.member_of_h = {}
        self.prefix = prefix.upper()
        self.grt = {
            'members': 'gr_mem',
        }
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname not in ['SunOS', 'Linux', 'HP-UX', 'AIX', 'OSF1']:
            print >>sys.stderr, 'module not supported on', self.sysname
            raise NotApplicable

        self.groups = {}
        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.groups.update(json.loads(os.environ[k]))
            except ValueError:
                print 'group syntax error on var[', k, '] = ', os.environ[k]

        if len(self.groups) == 0:
            raise NotApplicable

        if os.path.exists('/usr/xpg4/bin/id'):
            self.id_bin = '/usr/xpg4/bin/id'
        else:
            self.id_bin = 'id'

    def get_primary_group(self, user):
        cmd = [self.id_bin, "-gn", user]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        return out.strip()

    def member_of(self, user, refresh=False):
        if not refresh and user in self.member_of_h:
            # cache hit
            return self.member_of_h[user]

        eg = self.get_primary_group(user)
        if eg is None:
            self.member_of_h[user] = []
            return []

        cmd = [self.id_bin, "-Gn", user]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            self.member_of_h[user] = []
            return self.member_of_h[user]
        ag = set(out.strip().split())
        ag -= set([eg])
        self.member_of_h[user] = ag
        return self.member_of_h[user]

    def fixable(self):
        return RET_NA

    def del_member(self, group, user):
        ag = self.member_of(user)
        if len(ag) == 0:
            return 0
        g = ag - set([group])
        g = ','.join(g)
        return self.fix_member(g, user)

    def add_member(self, group, user):
        if 0 != self._check_member_accnt(user):
            print >>sys.stderr, 'group', group+':', 'cannot add inexistant user "%s"'%user
            return RET_ERR
        if self.get_primary_group(user) == group:
            print "%s is already the primary group of user %s: skip declaration as a secondary group (you may want to change your rule)" % (group, user)
            return RET_OK
        ag = self.member_of(user)
        g = ag | set([group])
        g = ','.join(g)
        return self.fix_member(g, user)

    def fix_member(self, g, user):
        cmd = ['usermod', '-G', g, user]
        print ' '.join(cmd)
        p = Popen(cmd)
        out, err = p.communicate()
        r = p.returncode
        ag = self.member_of(user, refresh=True)
        if r == 0:
            return RET_OK
        else:
            return RET_ERR

    def fix_members(self, group, target):
        r = 0
        for user in target:
            if group in self.member_of(user):
                continue
            r += self.add_member(group, user)
        return r

    def fix_item(self, group, item, target):
        if item == 'members':
            return self.fix_members(group, target)
        else:
            print >>sys.stderr, 'no fix implemented for', item
            return RET_ERR

    def _check_member_accnt(self, user):
        if which('getent'):
            xcmd = ['getent', 'passwd', user]
        elif which('pwget'):
            xcmd = ['pwget', '-n', user]
        else:
            return 0
        xp = Popen(xcmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        xout, xerr = xp.communicate()
        return xp.returncode

    def _check_members_accnts(self, group, list, which, verbose):
        r = RET_OK
        for user in list:
            rc = self._check_member_accnt(user)
            if rc != 0:
                r |= RET_ERR
                if verbose:
                    print >>sys.stderr, 'group', group, 'inexistant user "%s" which is %s'%(user, which)
        return r

    def filter_target(self, group, target):
        new_target = []
        for user in target:
            pg = self.get_primary_group(user)
            if pg == group:
                continue
            new_target.append(user)
        discarded = set(target)-set(new_target)
        if len(discarded) > 0:
            print "discarded %s from members of group %s, as they already use this group as primary (you may want to change your rule)" % (', '.join(discarded), group)
        return new_target
                
    def check_item(self, group, item, target, current, verbose=False):
        r = RET_OK
        if item == 'members':
            r |= self._check_members_accnts(group, current, 'INCLUDED', verbose)
            r |= self._check_members_accnts(group, target, 'REQUIRED', verbose)
        if not isinstance(current, list):
            current = [current]
        target = self.filter_target(group, target)
        if set(target) <= set(current):
            if verbose:
                print 'group', group, item+':', ', '.join(current)
            return r
        else:
            if verbose:
                print >>sys.stderr, 'group', group, item+':', ', '.join(current), '| target:', ', '.join(target)
            return r|RET_ERR

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
    try:
        o = CompGroupMembership(sys.argv[1])
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

