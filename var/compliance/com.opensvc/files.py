#!/opt/opensvc/bin/python
""" 
Verify file content. The collector provides the format with
wildcards. The module replace the wildcards with contextual
values.

The variable format is json-serialized:

{
  "path": "/some/path/to/file",
  "fmt": "root@corp.com		%%HOSTNAME%%@corp.com",
  "uid": 500,
  "gid": 500,
}

Wildcards:
%%ENV:VARNAME%%		Any environment variable value
%%HOSTNAME%%		Hostname
%%SHORT_HOSTNAME%%	Short hostname

"""

import os
import sys
import json
import stat
import re
import urllib
import tempfile
import pwd
import grp
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompFiles(object):
    def __init__(self, prefix='OSVC_COMP_FILES_'):
        self.prefix = prefix.upper()
        self._usr = {}
        self._grp = {}
        self.sysname, self.nodename, x, x, self.machine = os.uname()
        self.files = []

        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.files += self.add_file(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'failed to parse variable', os.environ[k]

        if len(self.files) == 0:
            print >>sys.stderr, "no applicable variable found in rulesets", self.prefix
            raise NotApplicable()

    def parse_fmt(self, d):
        fmt = d['fmt']
        p = re.compile('%%ENV:.+%%')
        for m in p.findall(fmt):
            s = m.strip("%").replace('ENV:', '')
            if s in os.environ:
                v = os.environ[s]
            elif 'OSVC_COMP_'+s in os.environ:
                v = os.environ['OSVC_COMP_'+s]
            else:
                print >>sys.stderr, s, 'is not an env variable'
                RET = RET_ERR
                return []
            fmt = fmt.replace(m, v)
        fmt = fmt.replace('%%HOSTNAME%%', self.nodename)
        fmt = fmt.replace('%%SHORT_HOSTNAME%%', self.nodename.split('.')[0])
        d['fmt'] = fmt
        return [d]

    def parse_ref(self, d):
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        fname, headers = urllib.urlretrieve(d['ref'], tmpf)
        if 'invalid file' in headers.values():
            print >>sys.stderr, d['ref'], "not found on collector"
            return RET_ERR
        d['fmt'] = f.read()
        f.close()
        return self.parse_fmt(d)

    def add_file(self, v):
        d = json.loads(v)
        if 'path' not in d:
            print >>sys.stderr, 'path should be in the dict:', d
            RET = RET_ERR
            return []
        if 'fmt' not in d and 'ref' not in d:
            print >>sys.stderr, 'fmt or ref should be in the dict:', d
            RET = RET_ERR
            return []
        if 'fmt' in d and 'ref' in d:
            print >>sys.stderr, 'fmt and ref are exclusive:', d
            RET = RET_ERR
            return []
        if 'fmt' in d:
            return self.parse_fmt(d)
        if 'ref' in d:
            return self.parse_ref(d)

    def fixable(self):
        return RET_NA

    def check_file_fmt(self, f, verbose=False):
        if verbose:
            cmd = ['diff', '-u', f['path'], '-']
        else:
            cmd = ['diff', '-q', f['path'], '-']
        p = Popen(cmd, stdin=PIPE)
        out, err = p.communicate(input=f['fmt'])
        return p.returncode

    def check_file_mode(self, f, verbose=False):
        if 'mode' not in f:
            return RET_OK
        try:
            mode = oct(stat.S_IMODE(os.stat(f['path']).st_mode))
        except:
            if verbose: print >>sys.stderr, f['path'], 'can not stat file'
            return RET_ERR
        mode = str(mode).lstrip("0")
        if mode != f['mode']:
            if verbose: print >>sys.stderr, f['path'], 'mode should be %s but is %s'%(f['mode'], mode)
            return RET_ERR
        return RET_OK

    def get_uid(self, uid):
        if uid in self._usr:
            return self._usr[uid]
        tuid = uid
        if isinstance(uid, (str, unicode)):
            try:
                info=pwd.getpwnam(uid)
                tuid = info[2]
                self._usr[uid] = tuid
            except:
                pass
        return tuid

    def get_gid(self, gid):
        if gid in self._grp:
            return self._grp[gid]
        tgid = gid
        if isinstance(gid, (str, unicode)):
            try:
                info=grp.getgrnam(gid)
                tgid = info[2]
                self._grp[gid] = tgid
            except:
                pass
        return tgid

    def check_file_uid(self, f, verbose=False):
        if 'uid' not in f:
            return RET_OK
        tuid = self.get_uid(f['uid'])
        uid = os.stat(f['path']).st_uid
        if uid != tuid:
            if verbose: print >>sys.stderr, f['path'], 'uid should be %s but is %s'%(tuid, str(uid))
            return RET_ERR
        return RET_OK

    def check_file_gid(self, f, verbose=False):
        if 'gid' not in f:
            return RET_OK
        tgid = self.get_gid(f['gid'])
        gid = os.stat(f['path']).st_gid
        if gid != tgid:
            if verbose: print >>sys.stderr, f['path'], 'gid should be %s but is %s'%(tgid, str(gid))
            return RET_ERR
        return RET_OK

    def check_file(self, f, verbose=False):
        if not os.path.exists(f['path']):
            print >>sys.stderr, f['path'], "does not exist"
            return RET_ERR
        r = 0
        r |= self.check_file_fmt(f, verbose)
        r |= self.check_file_mode(f, verbose)
        r |= self.check_file_uid(f, verbose)
        r |= self.check_file_gid(f, verbose)
        return r

    def fix_file_mode(self, f):
        if 'mode' not in f:
            return RET_OK
        if self.check_file_mode(f) == RET_OK:
            return RET_OK
        try:
            print "set %s mode to %s"%(f['path'], f['mode'])
            os.chmod(f['path'], int(f['mode'], 8))
        except:
            return RET_ERR
        return RET_OK

    def fix_file_owner(self, f):
        uid = -1
        gid = -1

        if 'uid' not in f and 'gid' not in f:
            return RET_OK
        if 'uid' in f and self.check_file_uid(f) != RET_OK:
            uid = self.get_uid(f['uid'])
        if 'gid' in f and self.check_file_gid(f) != RET_OK:
            gid = self.get_uid(f['gid'])
        if uid == -1 and gid == -1:
            return RET_OK
        try:
            print "set %s ownership to %d:%d"%(f['path'], uid, gid)
            os.chown(f['path'], uid, gid)
        except:
            return RET_ERR
        return RET_OK

    def fix_file_fmt(self, f):
        if self.check_file_fmt(f) == RET_OK:
            return RET_OK
        d = os.path.dirname(f['path'])
        if not os.path.exists(d):
           os.makedirs(d)
           try:
               os.chown(d, f['uid'], f['gid'])
           except:
               pass
        try:
            with open(f['path'], 'w') as fi:
                fi.write(f['fmt'])
        except:
            return RET_ERR
        return RET_OK

    def check(self):
        r = 0
        for f in self.files:
            r |= self.check_file(f, verbose=True)
        return r

    def fix(self):
        r = 0
        for f in self.files:
            r |= self.fix_file_fmt(f)
            r |= self.fix_file_mode(f)
            r |= self.fix_file_owner(f)
        return r


if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    o = CompFiles(sys.argv[1])
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

