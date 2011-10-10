#!/opt/opensvc/bin/python
""" 
module use OSVC_COMP_AUTHKEY_... vars
which define
{
 "action": "add",                # optional, defaults to "add"
 "authfile": "authorized_keys",  # optional, defaults to "authorized_keys2"
 "user": "foo",                  # mandatory
 "key": "XXXX..."                # mandatory
}
where:
- user: the username the key authorize to log as
- key: a single pub key to authorize
"""

import os
import sys
import json
import pwd
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompAuthKeys(object):
    def __init__(self, prefix='OSVC_COMP_AUTHKEY_', authfile="authorized_keys2"):
        self.prefix = prefix.upper()
        self.authkeys = []
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.authkeys += [json.loads(os.environ[k])]
            except ValueError:
                print >>sys.stderr, 'failed to concatenate', os.environ[k], 'to authkey list'

        if len(self.authkeys) == 0:
            print >>sys.stderr, "no applicable variable found in rulesets", self.prefix
            raise NotApplicable()

        self.installed_keys_d = {}
        if authfile not in ("authorized_key", "authorized_keys2"):
            print >>sys.stderr, "unsupported authfile:", authfile, "(use authorized_key or authorized_keys2)"
            raise NotApplicable()
        self.authfile = authfile

    def sanitize(self, ak):
        if 'user' not in ak:
            print >>sys.stderr, "no user set in rule"
            return False
        if 'key' not in ak:
            print >>sys.stderr, "no key set in rule"
            return False
        if 'action' not in ak:
            ak['action'] = 'add'
        if 'authfile' not in ak:
            ak['authfile'] = self.authfile
        if ak['authfile'] not in ("authorized_key", "authorized_keys2"):
            print >>sys.stderr, "unsupported authfile:", ak['authfile'], "(default to", self.authfile+")"
            ak['authfile'] = self.authfile
        return ak

    def fixable(self):
        return RET_NA

    def truncate_key(self, key):
        if len(key) < 50:
            return key
        else:
            return "'"+key[0:17] + "..." + key[-30:-1]+"'"

    def get_installed_keys(self, user):
        if user in self.installed_keys_d:
            return self.installed_keys_d[user]
        else:
            self.installed_keys_d[user] = []

        base = os.path.join(os.path.expanduser("~"+user), '.ssh')
        ps = [os.path.join(base, 'authorized_keys'),
              os.path.join(base, 'authorized_keys2')]
        for p in ps:
            if not os.path.exists(p):
                continue
            with open(p, 'r') as f:
                self.installed_keys_d[user] += f.read().split('\n')
        return self.installed_keys_d[user]

    def check_authkey(self, ak, verbose=True):
        ak = self.sanitize(ak)
        installed_keys = self.get_installed_keys(ak['user'])
        if ak['action'] == 'add':
            if ak['key'] not in installed_keys:
                if verbose:
                    print >>sys.stderr, 'key', self.truncate_key(ak['key']), 'must be installed for user', ak['user']
                r = RET_ERR
            else:
                if verbose:
                    print 'key', self.truncate_key(ak['key']), 'is correctly installed for user', ak['user']
                r = RET_OK
        elif ak['action'] == 'del':
            if ak['key'] in installed_keys:
                if verbose:
                    print >>sys.stderr, 'key', self.truncate_key(ak['key']), 'must be uninstalled for user', ak['user']
                r = RET_ERR
            else:
                if verbose:
                    print 'key', self.truncate_key(ak['key']), 'is correctly not installed for user', ak['user']
                r = RET_OK
        else:
            print >>sys.stderr, "unsupported action:", ak['action']
            return RET_ERR
        return r

    def fix_authkey(self, ak):
        ak = self.sanitize(ak)
        if ak['action'] == 'add':
            return self.add_authkey(ak)
        elif ak['action'] == 'del':
            return self.del_authkey(ak)
        else:
            print >>sys.stderr, "unsupported action:", ak['action']
            return RET_ERR

    def add_authkey(self, ak):
        if self.check_authkey(ak, verbose=False) == RET_OK:
            print 'key', self.truncate_key(ak['key']), 'is already installed for user', ak['user']
            return RET_OK

        base = os.path.join(os.path.expanduser("~"+ak['user']), '.ssh')
        p = os.path.join(base, 'authorized_keys2')

        try:
            userinfo=pwd.getpwnam(ak['user'])
        except KeyError:
            print 'user', ak['user'], 'does not exist'
            return RET_ERR

        if not os.path.exists(base):
            os.makedirs(base, 0700)
            os.chown(base, userinfo.pw_uid, userinfo.pw_gid)

        if not os.path.exists(p):
            with open(p, 'w') as f:
                f.write("")
                print p, "directory created"
                os.chmod(p, 0600)
                print p, "mode set to 0600"
                os.chown(p, userinfo.pw_uid, userinfo.pw_gid)
                print p, "ownetship set to %d:%d"%(userinfo.pw_uid, userinfo.pw_gid)

        with open(p, 'a') as f:
            f.write(ak['key'])
            if not ak['key'].endswith('\n'):
                f.write('\n')
            print 'key', self.truncate_key(ak['key']), 'installed for user', ak['user']

        return RET_OK

    def del_authkey(self, ak):
        if self.check_authkey(ak, verbose=False) == RET_OK:
            print 'key', self.truncate_key(ak['key']), 'is already not installed for user', ak['user']
            return RET_OK

        base = os.path.join(os.path.expanduser("~"+ak['user']), '.ssh')
        ps = [os.path.join(base, 'authorized_keys'),
              os.path.join(base, 'authorized_keys2')]

        for p in ps:
            if not os.path.exists(base):
                return RET_OK

            if not os.path.exists(p):
                return RET_OK

            with open(p, 'r') as f:
                l = f.read().split('\n')

            n = len(l)
            while True:
                try:
                    l.remove(ak['key'].replace('\n', ''))
                except ValueError:
                    break
            if len(l) == n:
                # nothing changed
                continue

            with open(p, 'w') as f:
                f.write('\n'.join(l))
                print 'key', self.truncate_key(ak['key']), 'uninstalled for user', ak['user']

        return RET_OK

    def check(self):
        r = 0
        for ak in self.authkeys:
            r |= self.check_authkey(ak)
        return r

    def fix(self):
        r = 0
        for ak in self.authkeys:
            r |= self.fix_authkey(ak)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = CompAuthKeys(sys.argv[1])
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

