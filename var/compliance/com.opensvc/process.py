#!/opt/opensvc/bin/python
""" 
Checks if a process is present, specifying its comm,
and optionnaly its owner's uid and/or username.

[{"command": "foo", "uid": "2345", "user": "foou"},
 {"command": "bar", "uid": "2345"},
 ...]
"""

import os
import sys
import json
import re
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *
from utilities import which

class CompProcess(object):
    def __init__(self, prefix='OSVC_COMP_PROCESS_'):
        self.prefix = prefix.upper()
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname not in ['Linux', 'AIX', 'SunOS', 'FreeBSD', 'Darwin', 'HP-UX']:
            print >>sys.stderr, 'module not supported on', self.sysname
            raise NotApplicable()

        if self.sysname == 'HP-UX' and 'UNIX95' not in os.environ:
            os.environ['UNIX95'] = ""

        self.process = []
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.process += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'failed to concatenate', os.environ[k], 'to process list'

        self.validate_process()

        if len(self.process) == 0:
            raise NotApplicable()

        self.load_ps()

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

    def load_ps_args(self):
        self.ps_args = {}
        cmd = ['ps', '-e', '-o', 'pid,uid,user,args']
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, "unable to fetch ps"
            raise ComplianceError
        lines = out.split('\n')
        if len(lines) < 2:
            return
        for line in lines[1:]:
            l = line.split()
            if len(l) < 4:
                continue
            pid, uid, user = l[:3]
            args = " ".join(l[3:])
            if args not in self.ps_args:
                self.ps_args[args] = [(pid, int(uid), user)]
            else:
                self.ps_args[args].append((pid, int(uid), user))

    def load_ps_comm(self):
        self.ps_comm = {}
        cmd = ['ps', '-e', '-o', 'comm,pid,uid,user']
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, "unable to fetch ps"
            raise ComplianceError
        lines = out.split('\n')
        if len(lines) < 2:
            return
        for line in lines[1:]:
            l = line.split()
            if len(l) != 4:
                continue
            comm, pid, uid, user = l
            if comm not in self.ps_comm:
                self.ps_comm[comm] = [(pid, int(uid), user)]
            else:
                self.ps_comm[comm].append((pid, int(uid), user))

    def load_ps(self):
        self.load_ps_comm()
        self.load_ps_args()

    def validate_process(self):
        l = []
        for process in self.process:
            if self._validate_process(process) == RET_OK:
                l.append(process)
        self.process = l

    def _validate_process(self, process):
        for i in 'comm', 'uid', 'args', 'user':
            if i not in process:
                continue
            process[i] = self.subst(process[i])
        if 'comm' not in process and 'args' not in process:
            print >>sys.stderr, process, 'rule is malformed ... nor comm nor args key present'
            return RET_ERR
        if 'uid' in process and type(process['uid']) != int:
            print >>sys.stderr, process, 'rule is malformed ... uid value must be integer'
            return RET_ERR
        return RET_OK

    def get_keys_args(self, args):
        found = []
        for key in self.ps_args:
            if re.match(args, key) is not None:
                found.append(key)
        return found

    def get_keys_comm(self, comm):
        found = []
        for key in self.ps_comm:
            if re.match(comm, key) is not None:
                found.append(key)
        return found

    def check_present_args(self, args, verbose):
        if len(args.strip()) == 0:
            return RET_OK
        found = self.get_keys_args(args)
        if len(found) == 0:
            if verbose:
                print >>sys.stderr, 'process with args', args, 'is not started ... should be'
            return RET_ERR
        else:
            if verbose:
                print 'process with args', args, 'is started ... on target'
        return RET_OK

    def check_present_comm(self, comm, verbose):
        if len(comm.strip()) == 0:
            return RET_OK
        found = self.get_keys_comm(comm)
        if len(found) == 0:
            if verbose:
                print >>sys.stderr, 'process with command', comm, 'is not started ... should be'
            return RET_ERR
        else:
            if verbose:
                print 'process with command', comm, 'is started ... on target'
        return RET_OK

    def check_present(self, process, verbose):
        r = RET_OK
        if 'comm' in process:
            r |= self.check_present_comm(process['comm'], verbose)
        if 'args' in process:
            r |= self.check_present_args(process['args'], verbose)
        return r

    def check_not_present_comm(self, comm, verbose):
        if len(comm.strip()) == 0:
            return RET_OK
        found = self.get_keys_comm(comm)
        if len(found) == 0:
           if verbose:
               print 'process with command', comm, 'is not started ... on target'
           return RET_OK
        else:
           if verbose:
               print >>sys.stderr, 'process with command', comm, 'is started ... shoud be'
        return RET_ERR

    def check_not_present_args(self, args, verbose):
        if len(args.strip()) == 0:
            return RET_OK
        found = self.get_keys_args(args)
        if len(found) == 0:
           if verbose:
               print 'process with args', args, 'is not started ... on target'
           return RET_OK
        else:
           if verbose:
               print >>sys.stderr, 'process with args', args, 'is started ... shoud be'
        return RET_ERR

    def check_not_present(self, process, verbose):
        r = 0
        if 'comm' in process:
            r |= self.check_not_present_comm(process['comm'], verbose)
        if 'args' in process:
            r |= self.check_not_present_args(process['args'], verbose)
        return r

    def check_process(self, process, verbose=True):
        r = RET_OK
        if process['state'] == 'on':
            r |= self.check_present(process, verbose)
            if r == RET_ERR:
                return RET_ERR
            if 'uid' in process:
                r |= self.check_uid(process, process['uid'], verbose)
            if 'user' in process:
                r |= self.check_user(process, process['user'], verbose)
        else:
            r |= self.check_not_present(process, verbose)

        return r

    def check_uid(self, process, uid, verbose):
        if 'args' in process:
            return self.check_uid_args(process['args'], uid, verbose)
        if 'comm' in process:
            return self.check_uid_comm(process['comm'], uid, verbose)

    def check_uid_comm(self, comm, uid, verbose):
        if len(comm.strip()) == 0:
            return RET_OK
        found = False
        keys = self.get_keys_comm(comm)
        for key in keys:
            for _pid, _uid, _user in self.ps_comm[key]:
                if uid == _uid:
                    found = True
                    continue
        if found:
            if verbose:
                print 'process with command', comm, 'runs with uid', _uid, '... on target'
        else:
            if verbose:
                print >>sys.stderr, 'process with command', comm, 'does not run with uid', _uid, '... should be'
            return RET_ERR
        return RET_OK

    def check_uid_args(self, args, uid, verbose):
        if len(args.strip()) == 0:
            return RET_OK
        found = False
        keys = self.get_keys_args(args)
        for key in keys:
            for _pid, _uid, _user in self.ps_args[key]:
                if uid == _uid:
                    found = True
                    continue
        if found:
            if verbose:
                print 'process with args', args, 'runs with uid', _uid, '... on target'
        else:
            if verbose:
                print >>sys.stderr, 'process with args', args, 'does not run with uid', _uid, '... should be'
            return RET_ERR
        return RET_OK

    def check_user(self, process, user, verbose):
        if 'args' in process:
            return self.check_user_args(process['args'], user, verbose)
        if 'comm' in process:
            return self.check_user_comm(process['comm'], user, verbose)

    def check_user_comm(self, comm, user, verbose):
        if len(comm.strip()) == 0:
            return RET_OK
        if user is None or len(user) == 0:
            return RET_OK
        found = False
        keys = self.get_keys_comm(comm)
        for key in keys:
            for _pid, _uid, _user in self.ps_comm[key]:
                if user == _user:
                    found = True
                    continue
        if found:
            if verbose:
                print 'process with command', comm, 'runs with user', _user, '... on target'
        else:
            if verbose:
                print >>sys.stderr, 'process with command', comm, 'runs with user', _user, '... should run with user', user
            return RET_ERR
        return RET_OK

    def check_user_args(self, args, user, verbose):
        if len(args.strip()) == 0:
            return RET_OK
        if user is None or len(user) == 0:
            return RET_OK
        found = False
        keys = self.get_keys_args(args)
        for key in keys:
            for _pid, _uid, _user in self.ps_args[key]:
                if user == _user:
                    found = True
                    continue
        if found:
            if verbose:
                print 'process with args', args, 'runs with user', _user, '... on target'
        else:
            if verbose:
                print >>sys.stderr, 'process with args', args, 'runs with user', _user, '... should run with user', user
            return RET_ERR
        return RET_OK

    def fix_process(self, process):
        if process['state'] == 'on':
            if self.check_present(process, verbose=False) == RET_OK:
                if ('uid' in process and self.check_uid(process, process['uid'], verbose=False) == RET_ERR) or \
                   ('user' in process and self.check_user(process, process['user'], verbose=False) == RET_ERR):
                    print >>sys.stderr, process, "runs with the wrong user. can't fix."
                    return RET_ERR
                return RET_OK
        elif process['state'] == 'off':
            if self.check_not_present(process, verbose=False) == RET_OK:
                return RET_OK

        if 'start' not in process or len(process['start'].strip()) == 0:
            print >>sys.stderr, "undefined fix method for process", process['comm']
            return RET_ERR

        v = process['start'].split(' ')
        if not which(v[0]):
            print >>sys.stderr, "fix command", v[0], "is not present or not executable"
            return RET_ERR
        print 'exec:', process['start']
        try:
            p = Popen(v, stdout=PIPE, stderr=PIPE)
            out, err = p.communicate()
        except Exception as e:
            print >>sys.stderr, e
            return RET_ERR
        if len(out) > 0:
            print out
        if len(err) > 0:
            print >>sys.stderr, err
        if p.returncode != 0:
            print >>sys.stderr, "fix up command returned with error code", p.returncode
            return RET_ERR
        return RET_OK

    def check(self):
        r = 0
        for process in self.process:
            r |= self.check_process(process)
        return r

    def fix(self):
        r = 0
        for process in self.process:
            r |= self.fix_process(process)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = CompProcess(sys.argv[1])
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

