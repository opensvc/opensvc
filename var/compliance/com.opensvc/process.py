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
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

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
            print "no applicable variable found in rulesets", self.prefix
            raise NotApplicable()

        self.load_ps()

    def load_ps(self):
        self.ps = {}
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
            if comm not in self.ps:
                self.ps[comm] = [(pid, int(uid), user)]
            else:
                self.ps[comm].append((pid, int(uid), user))

    def validate_process(self):
        l = []
        for process in self.process:
            if self._validate_process(process) == RET_OK:
                l.append(process)
        self.process = l

    def _validate_process(self, process):
        if 'comm' not in process:
            print >>sys.stderr, process, 'rule is malformed ... comm key not present'
            return RET_ERR
        if 'uid' in process and type(process['uid']) != int:
            print >>sys.stderr, process, 'rule is malformed ... uid value must be integer'
            return RET_ERR
        return RET_OK

    def check_present(self, comm, verbose):
        if comm not in self.ps:
           if verbose:
               print >>sys.stderr, 'process', comm, 'is not started ... should be'
           return RET_ERR
        else:
           if verbose:
               print 'process', comm, 'is started ... on target'
        return RET_OK

    def check_not_present(self, comm, verbose):
        if comm not in self.ps:
           if verbose:
               print 'process', comm, 'is not started ... on target'
           return RET_ERR
        else:
           if verbose:
               print >>sys.stderr, 'process', comm, 'is started ... shoud be'
        return RET_OK

    def check_process(self, process, verbose=True):
        r = RET_OK
        if process['state'] == 'on':
            r |= self.check_present(process['comm'], verbose)
            if r == RET_ERR:
                return RET_ERR
            if 'uid' in process:
                r |= self.check_uid(process['comm'], process['uid'], verbose)
            if 'user' in process:
                r |= self.check_user(process['comm'], process['user'], verbose)
        else:
            r |= self.check_not_present(process['comm'], verbose)

        return r

    def check_uid(self, comm, uid, verbose):
        found = False
        for _pid, _uid, _user in self.ps[comm]:
            if uid == _uid:
                found = True
                continue
        if found:
            if verbose:
                print 'process', comm, 'runs with uid', _uid, '... on target'
        else:
            if verbose:
                print >>sys.stderr, 'process', comm, 'does not run with uid', _uid, '... should be'
            return RET_ERR
        return RET_OK

    def check_user(self, comm, user, verbose):
        found = False
        for _pid, _uid, _user in self.ps[comm]:
            if user == _user:
                found = True
                continue
        if found:
            if verbose:
                print 'process', comm, 'runs with user', _user, '... on target'
        else:
            if verbose:
                print >>sys.stderr, 'process', comm, 'does not run with user', _user, '... should be'
            return RET_ERR
        return RET_OK

    def fix_process(self, process):
        if self.check_present(process['comm'], verbose=False) == RET_OK:
            if ('uid' in process and self.check_uid(process['comm'], process['uid'], verbose=False) == RET_ERR) or \
               ('user' in process and self.check_user(process['comm'], process['user'], verbose=False) == RET_ERR):
                print >>sys.stderr, process['comm'], "runs with the wrong user. can't fix."
                return RET_ERR
            return RET_OK
        if 'start' not in process:
            print >>sys.stderr, "undefined startup method for process", process['comm']
            return RET_ERR
        print 'exec:', process['start']
        p = Popen(process['start'].split(' '), stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, "start up command returned with error code", p.returncode
            print out
            print >>sys.stderr, err
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

