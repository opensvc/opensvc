#!/usr/bin/env /opt/opensvc/bin/python
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
import re
from subprocess import Popen 

sys.path.append(os.path.dirname(__file__))

from comp import *

blacklist = [
 "root",
 "bin",
 "daemon",
 "sys",
 "adm",
 "tty",
 "disk",
 "lp",
 "mem",
 "kmem",
 "wheel",
 "mail",
 "uucp",
 "man",
 "games",
 "gopher",
 "video",
 "dip",
 "ftp",
 "lock",
 "audio",
 "nobody",
 "users",
 "utmp",
 "utempter",
 "floppy",
 "vcsa",
 "cdrom",
 "tape",
 "dialout",
 "saslauth",
 "postdrop",
 "postfix",
 "sshd",
 "opensvc",
 "mailnull",
 "smmsp",
 "slocate",
 "rpc",
 "rpcuser",
 "nfsnobody",
 "tcpdump",
 "ntp"
]

class CompGroup(object):
    def __init__(self, prefix='OSVC_COMP_GROUP_'):
        self.prefix = prefix.upper()
        self.groupmod = 'groupmod'
        self.groupadd = 'groupadd'
        self.groupdel = 'groupdel'
        self.grt = {
            'gid': 'gr_gid',
        }

        self.groupmod_p = {
            'gid': '-g',
        }

        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname == 'AIX':
            self.groupmod = 'chgroup'
            self.groupadd = 'mkgroup'
            self.groupdel = 'rmgroup'
            self.groupmod_p = {
                'gid': 'id',
            }

        if self.sysname not in ['SunOS', 'Linux', 'HP-UX', 'AIX', 'OSF1']:
            print >>sys.stderr, 'module not supported on', self.sysname
            raise NotApplicable

        self.groups = {}
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.groups.update(json.loads(os.environ[k]))
            except ValueError:
                print >>sys.stderr, 'group syntax error on var[', k, '] = ', os.environ[k]

        if len(self.groups) == 0:
            raise NotApplicable

        p = re.compile('%%ENV:\w+%%')
        for group, d in self.groups.items():
            for k in d:
                if type(d[k]) not in [str, unicode]:
                    continue
                for m in p.findall(d[k]):
                    s = m.strip("%").replace('ENV:', '')
                    if s in os.environ:
                        v = os.environ[s]
                    elif 'OSVC_COMP_'+s in os.environ:
                        v = os.environ['OSVC_COMP_'+s]
                    else:
                        print >>sys.stderr, s, 'is not an env variable'
                        raise NotApplicable()
                    d[k] = d[k].replace(m, v)
                if k in ('uid', 'gid'):
                    d[k] = int(d[k])


    def fixable(self):
        return RET_NA

    def fmt_opt_gen(self, item, target):
        return [item, target]

    def fmt_opt_aix(self, item, target):
        return ['='.join((item, target))]

    def fmt_opt(self, item, target):
        if self.sysname == 'AIX':
            return self.fmt_opt_aix(item, target)
        else:
            return self.fmt_opt_gen(item, target)
        
    def fix_item(self, group, item, target):
        if item in self.groupmod_p:
            cmd = [getattr(self, 'groupmod')]
            cmd += self.fmt_opt(self.groupmod_p[item], str(target))
            cmd += [group]
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
        if type(current) == int and current < 0:
            current += 4294967296
        if target == current:
            if verbose:
                print 'group', group, item+':', current
            return RET_OK
        else:
            if verbose:
                print >>sys.stderr, 'group', group, item+':', current, 'target:', target
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

    def check_group_del(self, group):
        try:
            groupinfo = grp.getgrnam(group)
        except KeyError:
            print 'group', group, 'does not exist, on target'
            return RET_OK
        print >>sys.stderr, 'group', group, "exists, shouldn't"
        return RET_ERR

    def check_group(self, group, props):
        if group.startswith('-'):
            return self.check_group_del(group.lstrip('-'))
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
        cmd = [getattr(self, 'groupadd')]
        for item in self.grt:
            cmd += self.fmt_opt(self.groupmod_p[item], str(props[item]))
        cmd += [group]
        print ' '.join(cmd)
        p = Popen(cmd)
        out, err = p.communicate()
        r = p.returncode
        if r == 0:
            return RET_OK
        else:
            return RET_ERR

    def fix_group_del(self, group):
        if group in blacklist:
            print >>sys.stderr, "delete", group, "... cowardly refusing"
            return RET_ERR
        try:
            groupinfo = grp.getgrnam(group)
        except KeyError:
            return RET_OK
        cmd = [self.groupdel, group]
        print ' '.join(cmd)
        p = Popen(cmd)
        out, err = p.communicate()
        r = p.returncode
        if r == 0:
            return RET_OK
        else:
            return RET_ERR

    def fix_group(self, group, props):
        if group.startswith('-'):
            return self.fix_group_del(group.lstrip('-'))
        r = 0
        try:
            groupinfo = grp.getgrnam(group)
        except KeyError:
            if self.try_create_group(props):
                return self.create_group(group, props)
            else:
                print >>sys.stderr, 'group', group, 'does not exist'
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
        o = CompGroup(sys.argv[1])
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

