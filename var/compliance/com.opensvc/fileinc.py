#!/usr/bin/env /opt/opensvc/bin/python
""" 
Verify file content. The collector provides the format with
wildcards. The module replace the wildcards with contextual
values.

The variable format is json-serialized:

{
  "path": "/some/path/to/file",
  "check": "some pattern into the file",
  "fmt": "full added content with %%HOSTNAME%%@corp.com",
 - or - 
  "ref": "http:// ..."
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
import codecs

sys.path.append(os.path.dirname(__file__))

from comp import *

MAXSZ = 8*1024*1024

class FileInc(object):
    def __init__(self, prefix='OSVC_COMP_FILEINC_'):
        self.prefix = prefix.upper()
        self.files = {}
        self.ok = {}
        self.checks = []
        self.upds = {}

        self.sysname, self.nodename, x, x, self.machine = os.uname()

        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.add_file(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'key syntax error on var[', k, '] = ',os.environ[k]

        if len(self.checks) == 0:
            raise NotApplicable()

    def fixable(self):
        return RET_NA

    def parse_fmt(self, x):
        if isinstance(x, int):
            x = str(x)
        x = x.replace('%%HOSTNAME%%', self.nodename)
        x = x.replace('%%SHORT_HOSTNAME%%', self.nodename.split('.')[0])
        return x

    def parse_ref(self, url):
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        try:
            fname, headers = urllib.urlretrieve(url, tmpf)
            if 'invalid file' in headers.values():
                print >>sys.stderr, url, "not found on collector"
                return RET_ERR
            content = unicode(f.read())
            f.close()
        except:
             print >>sys.stderr, url, "not found on collector"
             return ''
        if '<title>404 Not Found</title>' in content:
            print >>sys.stderr, url, "not found on collector"
            return ''
        return self.parse_fmt(content)

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

    def read_file(self, path):
        if not os.path.exists(path):
            return ''
        out = ''
        try :
            f = codecs.open(path, 'r', encoding="utf8", errors="ignore")
            out = f.read().rstrip('\n')
            f.close()
        except IOError as (errno, strerror):
            print "cannot read '%s', error=%d - %s" %(path, errno, strerror)
            raise
        except:
            print >>sys.stderr, "Cannot open '%s', unexpected error: %s"%(path, sys.exc_info()[0])
            raise
        return out

    def add_file(self,v):
        r = RET_OK
        d = json.loads(v)
        if 'path' not in d:
            print >>sys.stderr, "'path' should be defined:", d
            r |= RET_ERR
        if 'fmt' in d and 'ref' in d:
            print >>sys.stderr, "'fmt' and 'ref' are exclusive:", d
            r |= RET_ERR
        for k in ('path', 'check', 'fmt', 'ref'):
            if k in d:
                d[k] = self.subst(d[k])
        if 'path' in d:
            d['path'] = d['path'].strip()
        if 'ref' in d:
            d['ref'] = d['ref'].strip()
        if not d['path'] in self.upds:
            self.upds[d['path']] = 0
        if not d['path'] in self.files:
            try:
                fsz = os.path.getsize(d['path'])
            except:
                fsz = 0
            if fsz > MAXSZ:
                self.ok[d['path']] = 0
                self.files[d['path']] = ''
                print >>sys.stderr, "file '%s' is too large [%.2f Mb] to fit" %(d['path'], fsz/(1024.*1024))
                r |= RET_ERR
            else:
                try:
                    self.files[d['path']] = self.read_file(d['path'])
                    self.ok[d['path']] = 1
                except:
                    self.files[d['path']] = ""
                    self.ok[d['path']] = 0
                    r |= RET_ERR
        c = ''
        if 'fmt' in d:
            c = self.parse_fmt(d['fmt'])
        elif 'ref' in d:
            c = self.parse_ref(d['ref'])
        else:
            print >>sys.stderr, "'fmt' or 'ref' should be defined:", d
            r |= RET_ERR
        c = c.strip()
        if re.match(d['check'], c) is not None or len(c) == 0:
            val = True
        else:
            val = False
            r |= RET_ERR
        self.checks.append({'check':d['check'], 'path':d['path'], 'add':c, 'valid':val})
        return r
        
    def check(self):
        r = RET_OK
        for ck in self.checks:
            if not ck['valid']:
                print >>sys.stderr, "rule error: '%s' does not match target content" % ck['check']
                r |= RET_ERR
                continue
            if self.ok[ck['path']] != 1:
                r |= RET_ERR
                continue
            pr = RET_OK
            m = 0
            ok = 0
            lines = self.files[ck['path']].split('\n')
            for line in lines:
                if re.match(ck['check'], line):
                    m += 1
                    if len(ck['add']) > 0 and line == ck['add']:
                        print "line '%s' found in '%s'" %(line, ck['path'])
                        ok += 1
                    if m > 1:
                        print >>sys.stderr, "duplicate match of pattern '%s' in '%s'"%(ck['check'], ck['path'])
                        pr |= RET_ERR
            if len(ck['add']) == 0:
                if m > 0:
                    print >>sys.stderr, "pattern '%s' found in %s"%(ck['check'], ck['path'])
                    pr |= RET_ERR
                else:
                    print "pattern '%s' not found in %s"%(ck['check'], ck['path'])
            elif ok == 0:
                print >>sys.stderr, "line '%s' not found in %s"%(ck['add'], ck['path'])
                pr |= RET_ERR
            elif m == 0:
                print >>sys.stderr, "pattern '%s' not found in %s"%(ck['check'], ck['path'])
                pr |= RET_ERR
            r |= pr
        return r

    def rewrite_files(self):
        r = RET_OK
        for path in self.files:
            if self.upds[path] == 0:
                continue
            if self.ok[path] != 1:
                r |= RET_ERR
                continue
            if not os.path.exists(path):
                print >>sys.stderr, "'%s' will be created, please check owner and permissions" %path
            try:
                f = codecs.open(path, 'w', encoding="utf8")
                f.write(self.files[path])
                f.close()
                print "'%s' successfully rewritten" %path
            except:
                print >>sys.stderr, "failed to rewrite '%s'" %path
                r |= RET_ERR
        return r

    def fix(self):
        r = RET_OK
        for ck in self.checks:
            if not ck['valid']:
                print >>sys.stderr, "rule error: '%s' does not match target content" % ck['check']
                r |= RET_ERR
                continue
            if self.ok[ck['path']] != 1:
                r |= RET_ERR
                continue
            need_rewrite = False
            m = 0
            lines = self.files[ck['path']].rstrip('\n').split('\n')
            for i, line in enumerate(lines):
                if re.match(ck['check'], line):
                    m += 1
                    if m == 1:
                        if line != ck['add']:
                            # rewrite line
                            print "rewrite %s:%d:'%s', new content: '%s'" %(ck['path'], i, line, ck['add'])
                            lines[i] = ck['add']
                            need_rewrite = True
                    elif m > 1:
                        # purge dup
                        print "remove duplicate line %s:%d:'%s'" %(ck['path'], i, line)
                        lines[i] = ""
                        need_rewrite = True
            if m == 0 and len(ck['add']) > 0:
                print "add line '%s' to %s"%(ck['add'], ck['path'])
                lines.append(ck['add'])
                need_rewrite = True

            if need_rewrite:
                self.files[ck['path']] = '\n'.join(lines).rstrip("\n")+"\n"
                self.upds[ck['path']] = 1

        r |= self.rewrite_files()
        return r


if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = FileInc(sys.argv[1])
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
    except ComplianceError:
        sys.exit(RET_ERR)
    except NotApplicable:
        sys.exit(RET_NA)
    except:
        import traceback
        traceback.print_exc()
        sys.exit(RET_ERR)

    sys.exit(RET)

