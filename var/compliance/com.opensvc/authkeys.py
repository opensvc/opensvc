#!/usr/bin/env /opt/opensvc/bin/python
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
import pwd, grp
import codecs
import re
import datetime
import shutil
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompAuthKeys(object):
    def __init__(self, prefix='OSVC_COMP_AUTHKEY_', authfile="authorized_keys2"):
        self.prefix = prefix.upper()
        self.authkeys = []
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            s = self.subst(os.environ[k])
            try:
                self.authkeys += [json.loads(s)]
            except ValueError:
                print >>sys.stderr, 'failed to concatenate', os.environ[k], 'to authkey list'

        if len(self.authkeys) == 0:
            raise NotApplicable()

        for ak in self.authkeys:
            ak['key'] = ak['key'].replace('\n', '')

        self.installed_keys_d = {}
        if authfile not in ("authorized_keys", "authorized_keys2"):
            print >>sys.stderr, "unsupported authfile:", authfile, "(use authorized_keys or authorized_keys2)"
            raise NotApplicable()
        self.authfile = authfile

        self.allowusers_check_done = []
        self.allowusers_fix_todo = []
        self.allowgroups_check_done = []
        self.allowgroups_fix_todo = []

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
        if ak['authfile'] not in ("authorized_keys", "authorized_keys2"):
            print >>sys.stderr, "unsupported authfile:", ak['authfile'], "(default to", self.authfile+")"
            ak['authfile'] = self.authfile
        for key in ('user', 'key', 'action', 'authfile'):
            ak[key] = ak[key].strip()
        return ak

    def fixable(self):
        return RET_NA

    def truncate_key(self, key):
        if len(key) < 50:
            s = key
        else:
            s = "'"+key[0:17] + "..." + key[-30:]+"'"
        return s.encode('utf8')

    def reload_sshd(self):
        cmd = ['ps', '-ef']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, "can not find sshd process"
            return RET_ERR
        lines = out.split('\n')
        for line in lines:
            if not line.endswith('sbin/sshd'):
                continue
            l = line.split()
            pid = int(l[1])
            name = l[-1]
            print "send sighup to pid %d (%s)" % (pid, name)
            os.kill(pid, 1)
            return RET_OK
        print >>sys.stderr, "can not find sshd process to signal"
        return RET_ERR

    def get_sshd_config(self):
        cfs = []
        if hasattr(self, "cache_sshd_config_f"):
            return self.cache_sshd_config_f

        cmd = ['ps', '-eo', 'comm']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            l = out.split('\n')
            if '/usr/local/sbin/sshd' in l:
                cfs.append(os.path.join(os.sep, 'usr', 'local', 'etc', 'sshd_config'))
            if '/usr/sfw/sbin/sshd' in l:
                cfs.append(os.path.join(os.sep, 'etc', 'sshd_config'))

        cfs += [os.path.join(os.sep, 'etc', 'ssh', 'sshd_config'),
                os.path.join(os.sep, 'opt', 'etc', 'sshd_config'),
                os.path.join(os.sep, 'etc', 'opt', 'ssh', 'sshd_config'),
                os.path.join(os.sep, 'usr', 'local', 'etc', 'sshd_config')]
        cf = None
        for _cf in cfs:
            if os.path.exists(_cf):
                cf = _cf
                break
        self.cache_sshd_config_f = cf
        if cf is None:
            print >>sys.stderr, "sshd_config not found"
            return None
        return cf

    def _get_authkey_file(self, key):
        if key == "authorized_keys":
            # default
            return ".ssh/authorized_keys"
        elif key == "authorized_keys2":
            key = "AuthorizedKeysFile"
        else:
            print >>sys.stderr, "unknown key", key
            return None


        cf = self.get_sshd_config()
        if cf is None:
            print >>sys.stderr, "sshd_config not found"
            return None
        with open(cf, 'r') as f:
            buff = f.read()
        for line in buff.split('\n'):
            l = line.split()
            if len(l) != 2:
                continue
            if l[0].strip() == key:
                return l[1]
        # not found, return default
        return ".ssh/authorized_keys2"

    def get_allowusers(self):
        if hasattr(self, "cache_allowusers"):
            return self.cache_allowusers
        cf = self.get_sshd_config()
        if cf is None:
            print >>sys.stderr, "sshd_config not found"
            return None
        with open(cf, 'r') as f:
            buff = f.read()
        for line in buff.split('\n'):
            l = line.split()
            if len(l) < 2:
                continue
            if l[0].strip() == "AllowUsers":
                self.cache_allowusers = l[1:]
                return l[1:]
        self.cache_allowusers = None
        return None

    def get_allowgroups(self):
        if hasattr(self, "cache_allowgroups"):
            return self.cache_allowgroups
        cf = self.get_sshd_config()
        if cf is None:
            print >>sys.stderr, "sshd_config not found"
            return None
        with open(cf, 'r') as f:
            buff = f.read()
        for line in buff.split('\n'):
            l = line.split()
            if len(l) < 2:
                continue
            if l[0].strip() == "AllowGroups":
                self.cache_allowgroups = l[1:]
                return l[1:]
        self.cache_allowgroups = None
        return None

    def get_authkey_file(self, key, user):
        p = self._get_authkey_file(key)
        if p is None:
            return None
        p = p.replace('%u', user)
        p = p.replace('%h', os.path.expanduser('~'+user))
        p = p.replace('~', os.path.expanduser('~'+user))
        if not p.startswith('/'):
            p = os.path.join(os.path.expanduser('~'+user), p)
        return p

    def get_authkey_files(self, user):
        l = []
        p = self.get_authkey_file('authorized_keys', user)
        if p is not None:
            l.append(p)
        p = self.get_authkey_file('authorized_keys2', user)
        if p is not None:
            l.append(p)
        return l

    def get_installed_keys(self, user):
        if user in self.installed_keys_d:
            return self.installed_keys_d[user]
        else:
            self.installed_keys_d[user] = []

        ps = self.get_authkey_files(user)
        for p in ps:
            if not os.path.exists(p):
                continue
            with codecs.open(p, 'r', encoding="utf8", errors="ignore") as f:
                self.installed_keys_d[user] += f.read().split('\n')
        return self.installed_keys_d[user]

    def get_user_group(self, user):
        gid = pwd.getpwnam(user).pw_gid
        try:
            gname = grp.getgrgid(gid).gr_name
        except KeyError:
            gname = None
        return gname

    def fix_allowusers(self, ak, verbose=True):
        self.check_allowuser(ak, verbose=False)
        if not ak['user'] in self.allowusers_fix_todo:
            return RET_OK
        self.allowusers_fix_todo.remove(ak['user'])
        au = self.get_allowusers()
        if au is None:
            return RET_OK
        l = ["AllowUsers"] + au + [ak['user']]
        s = " ".join(l)

        print "adding", ak['user'], "to currently allowed users"
        cf = self.get_sshd_config()
        if cf is None:
            print >>sys.stderr, "sshd_config not found"
            return None
        with open(cf, 'r') as f:
            buff = f.read()
        lines = buff.split('\n')
        for i, line in enumerate(lines):
            l = line.split()
            if len(l) < 2:
                continue
            if l[0].strip() == "AllowUsers":
                lines[i] = s
        buff = "\n".join(lines)
        backup = cf+'.'+str(datetime.datetime.now())
        shutil.copy(cf, backup)
        with open(cf, 'w') as f:
            f.write(buff)
        self.reload_sshd()
        return RET_OK

    def fix_allowgroups(self, ak, verbose=True):
        self.check_allowgroup(ak, verbose=False)
        if not ak['user'] in self.allowgroups_fix_todo:
            return RET_OK
        self.allowgroups_fix_todo.remove(ak['user'])
        ag = self.get_allowgroups()
        if ag is None:
            return RET_OK
        ak['group'] = self.get_user_group(ak['user'])
        if ak['group'] is None:
            print >>sys.stderr, "can not set AllowGroups in sshd_config: primary group of user %s not found" % ak['user']
            return RET_ERR
        l = ["AllowGroups"] + ag + [ak['group']]
        s = " ".join(l)

        print "adding", ak['group'], "to currently allowed groups"
        cf = self.get_sshd_config()
        if cf is None:
            print >>sys.stderr, "sshd_config not found"
            return RET_ERR
        with open(cf, 'r') as f:
            buff = f.read()
        lines = buff.split('\n')
        for i, line in enumerate(lines):
            l = line.split()
            if len(l) < 2:
                continue
            if l[0].strip() == "AllowGroups":
                lines[i] = s
        buff = "\n".join(lines)
        backup = cf+'.'+str(datetime.datetime.now())
        shutil.copy(cf, backup)
        with open(cf, 'w') as f:
            f.write(buff)
        self.reload_sshd()
        return RET_OK

    def check_allowuser(self, ak, verbose=True):
        if ak['user'] in self.allowusers_check_done:
            return RET_OK
        self.allowusers_check_done.append(ak['user'])
        au = self.get_allowusers()
        if au is None:
            return RET_OK
        elif ak['user'] in au:
            if verbose:
                print ak['user'], "is correctly set in sshd AllowUsers"
            r = RET_OK
        else:
            if verbose:
                print >>sys.stderr, ak['user'], "is not set in sshd AllowUsers"
            self.allowusers_fix_todo.append(ak['user'])
            r = RET_ERR
        return r

    def check_allowgroup(self, ak, verbose=True):
        if ak['user'] in self.allowgroups_check_done:
            return RET_OK
        self.allowgroups_check_done.append(ak['user'])
        ag = self.get_allowgroups()
        if ag is None:
            return RET_OK
        ak['group'] = self.get_user_group(ak['user'])
        if ak['group'] is None:
            if verbose:
                print >>sys.stderr, "can not determine primary group of user %s to add to AllowGroups" % ak['user']
            return RET_ERR
        elif ak['group'] in ag:
            if verbose:
                print ak['group'], "is correctly set in sshd AllowGroups"
            r = RET_OK
        else:
            if verbose:
                print >>sys.stderr, ak['group'], "is not set in sshd AllowGroups"
            self.allowgroups_fix_todo.append(ak['user'])
            r = RET_ERR
        return r

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
            r = self.add_authkey(ak)
            return r
        elif ak['action'] == 'del':
            return self.del_authkey(ak)
        else:
            print >>sys.stderr, "unsupported action:", ak['action']
            return RET_ERR

    def add_authkey(self, ak):
        if self.check_authkey(ak, verbose=False) == RET_OK:
            return RET_OK

        try:
            userinfo=pwd.getpwnam(ak['user'])
        except KeyError:
            print >>sys.stderr, 'user', ak['user'], 'does not exist'
            return RET_ERR

        p = self.get_authkey_file(ak['authfile'], ak['user'])
        if p is None:
            print >>sys.stderr, "could not determine", ak['authfile'], "location"
            return RET_ERR
        base = os.path.dirname(p)

        if not os.path.exists(base):
            os.makedirs(base, 0700)
            print base, "created"
            if p.startswith(os.path.expanduser('~'+ak['user'])):
                os.chown(base, userinfo.pw_uid, userinfo.pw_gid)
                print base, "ownership set to %d:%d"%(userinfo.pw_uid, userinfo.pw_gid)

        if not os.path.exists(p):
            with open(p, 'w') as f:
                f.write("")
                print p, "created"
                os.chmod(p, 0600)
                print p, "mode set to 0600"
                os.chown(p, userinfo.pw_uid, userinfo.pw_gid)
                print p, "ownetship set to %d:%d"%(userinfo.pw_uid, userinfo.pw_gid)

        with open(p, 'a') as f:
            f.write(ak['key'].encode('utf8'))
            if not ak['key'].endswith('\n'):
                f.write('\n')
            print 'key', self.truncate_key(ak['key']), 'installed for user', ak['user']

        return RET_OK

    def del_authkey(self, ak):
        if self.check_authkey(ak, verbose=False) == RET_OK:
            print 'key', self.truncate_key(ak['key']), 'is already not installed for user', ak['user']
            return RET_OK

        ps = self.get_authkey_files(ak['user'])

        for p in ps:
            base = os.path.basename(p)
            if not os.path.exists(p):
                continue

            with codecs.open(p, 'r', encoding="utf8", errors="ignore") as f:
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
                f.write('\n'.join(l).encode('utf8'))
                print 'key', self.truncate_key(ak['key']), 'uninstalled for user', ak['user']

        return RET_OK

    def check(self):
        r = 0
        for ak in self.authkeys:
            r |= self.check_authkey(ak)
            if ak['action'] == 'add':
                r |= self.check_allowgroup(ak)
                r |= self.check_allowuser(ak)
        return r

    def fix(self):
        r = 0
        for ak in self.authkeys:
            r |= self.fix_authkey(ak)
            if ak['action'] == 'add':
                r |= self.fix_allowgroups(ak)
                r |= self.fix_allowusers(ak)
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

