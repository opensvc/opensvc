#!/opt/opensvc/bin/python
""" 
module use OSVC_COMP_USER_... vars
which define {'username':{'propname':'propval',... }, ...}

example: 
{"tibco":{"shell":"/bin/ksh","gecos":"agecos",},
 "tibco1":{"shell":"/bin/tcsh","gecos":"another gecos",},
}

supported dictionnary keys:
- uid
- gid
- gecos
- homedir
- shell
"""

import os
import sys
import json
import pwd
from subprocess import Popen, list2cmdline

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompUser(object):
    def __init__(self, prefix='OSVC_COMP_USER_'):
        self.prefix = prefix
        self.pwt = {
            'shell': 'pw_shell',
            'homedir': 'pw_dir',
            'uid': 'pw_uid',
            'gid': 'pw_gid',
            'gecos': 'pw_gecos',
        }
        self.usermod_p = {
            'shell': '-s',
            'homedir': '-m -d',
            'uid': '-u',
            'gid': '-g',
            'gecos': '-c',
        }
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname not in ['SunOS', 'Linux']:
            print >>sys.stderr, 'module not supported on', self.sysname
            raise NotApplicable()

        self.users = {}
        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.users.update(json.loads(os.environ[k]))
            except ValueError:
                print >>sys.stderr, 'user syntax error on var[', k, '] = ',os.environ[k]

        if len(self.users) == 0:
            print >>sys.stderr, "no applicable variable found in rulesets", self.prefix
            raise NotApplicable()

    def fixable(self):
        return RET_NA

    def fix_item(self, user, item, target):
        cmd = ['usermod'] + self.usermod_p[item].split() + [str(target), user]
        print list2cmdline(cmd)
        p = Popen(cmd)
        out, err = p.communicate()
        r = p.returncode
        if r == 0:
            return RET_OK
        else:
            return RET_ERR

    def check_item(self, user, item, target, current, verbose=False):
        if target == current:
            if verbose:
                print 'OK: user:', user, item+':', current
            return RET_OK
        else:
            if verbose:
                print >>sys.stderr, 'user:', user, item+':', current, 'target:', target
            return RET_ERR

    def check_user(self, user, props):
        r = 0
        try:
            userinfo=pwd.getpwnam(user)
        except KeyError:
            if self.try_create_user(props):
                print >>sys.stderr, 'user', user, 'does not exist'
                return RET_ERR
            else:
                print 'user', user, 'does not exist and not enough info to create it'
                return RET_OK
        for prop in self.pwt:
            if prop in props:
                r |= self.check_item(user, prop, props[prop], getattr(userinfo, self.pwt[prop]), verbose=True)
        return r

    def try_create_user(self, props):
        #
        # don't try to create user if passwd db is not 'files'
        # beware: 'files' db is the implicit default
        #
        if 'db' in props and props['db'] != 'files':
            return False
        if set(self.pwt.keys()) <= set(props.keys()):
            return True
        return False

    def create_user(self, user, props):
        cmd = ['useradd']
        for item in self.pwt:
            prop = str(props[item])
            if len(prop) == 0:
                continue
            cmd = cmd + self.usermod_p[item].split() + [prop]
        cmd += [user]
        print list2cmdline(cmd)
        p = Popen(cmd)
        out, err = p.communicate()
        r = p.returncode
        if r == 0:
            return RET_OK
        else:
            return RET_ERR

    def fix_user(self, user, props):
        r = 0
        try:
            userinfo=pwd.getpwnam(user)
        except KeyError:
            if self.try_create_user(props):
                return self.create_user(user, props)
            else:
                print 'user', user, 'does not exist and not enough info to create it'
                return RET_OK
        for prop in self.pwt:
            if prop in props and \
               self.check_item(user, prop, props[prop], getattr(userinfo, self.pwt[prop])) != RET_OK:
                r |= self.fix_item(user, prop, props[prop])
        return r

    def check(self):
        r = 0
        for user, props in self.users.items():
            r |= self.check_user(user, props)
        return r

    def fix(self):
        r = 0
        for user, props in self.users.items():
            r |= self.fix_user(user, props)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    o = CompUser(sys.argv[1])
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

