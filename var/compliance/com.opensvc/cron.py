#!/opt/opensvc/bin/python

import os
import sys
import shutil
import glob
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompCron(object):
    def __init__(self, prefix='OSVC_COMP_CRON_ENTRY_'):
        self.prefix = prefix.upper()
        self.ce = []
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname == 'SunOS' :
            self.crontab_locs = [
                '/var/spool/cron/crontabs'
            ]
        else:
            self.crontab_locs = [
                '/etc/cron.d',
                '/var/spool/cron/crontabs',
                '/var/spool/cron',
                '/var/cron/tabs',
            ]

        for k in os.environ:
            if k[:len(prefix)] == prefix:
                e = os.environ[k].split(':')
                if len(e) < 5:
                    print >>sys.stderr, "malformed variable %s. format: action:user:sched:cmd:[file]"%k
                    continue
                if e[0] not in ('add', 'del'):
                    print >>sys.stderr, "unsupported action in variable %s. set 'add' or 'del'"%k
                    continue
                if len(e[2].split()) != 5:
                    print >>sys.stderr, "malformed schedule in variable %s"%k
                    continue
                self.ce += [{
                        'var': k,
                        'action': e[0],
                        'user': e[1],
                        'sched': e[2],
                        'cmd': e[3],
                        'file': e[4],
                       }]

        if len(self.ce) == 0:
            raise NotApplicable()


    def activate_cron(self, cron_file):
        """ Activate changes (actually only needed on HP-UX)
        """
        if '/var/spool/' in cron_file:
            print "tell crond about the change"
            cmd = ['crontab', cron_file]
            process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
            buff = process.communicate()

    def fixable(self):
        r = RET_OK
        for e in self.ce:
            try:
                self._fixable_cron(e)
            except ComplianceError, e:
                print >>sys.stderr, str(e)
                r = RET_ERR
            except Unfixable, e:
                print >>sys.stderr, str(e)
                return r
        return r

    def fix(self):
        r = RET_OK
        for e in self.ce:
            try:
                if e['action'] == 'add':
                    self._add_cron(e)
                elif e['action'] == 'del':
                    self._del_cron(e)
            except ComplianceError, e:
                print >>sys.stderr, str(e)
                r = RET_ERR
            except Unfixable, e:
                print >>sys.stderr, str(e)
                return r
        return r

    def check(self):
        r = RET_OK
        for e in self.ce:
            try:
                self._check_cron(e)
            except ComplianceError, e:
                print >>sys.stderr, str(e)
                r = RET_ERR
            except Unfixable, e:
                print >>sys.stderr, str(e)
                return r
        return r

    def get_cron_file(self, e):
        """ order of preference
        """
        cron_file = None
        for loc in self.crontab_locs:
            if not os.path.exists(loc):
                continue
            if loc == '/etc/cron.d':
                 cron_file = os.path.join(loc, e['file'])
            else:
                 cron_file = os.path.join(loc, e['user'])
            break
        return cron_file

    def format_entry(self, cron_file, e):
        if 'cron.d' in cron_file:
            s = ' '.join([e['sched'], e['user'], e['cmd']])
        else:
            s = ' '.join([e['sched'], e['cmd']])
        return s

    def _fixable_cron(self, e):
        cron_file = self.get_cron_file(e)

        if cron_file is None:
            raise Unfixable("no crontab usual location found (%s)"%str(self.crontab_locs))

    def _check_cron(self, e):
        cron_file = self.get_cron_file(e)

        if cron_file is None:
            raise Unfixable("no crontab usual location found (%s)"%str(self.crontab_locs))

        s = self.format_entry(cron_file, e)

        if not os.path.exists(cron_file):
            raise ComplianceError("cron entry not found '%s' in '%s'"%(s, cron_file))

        with open(cron_file, 'r') as f:
            new = f.readlines()
            found = False
            for line in new:
                if s == line[:-1]:
                     found = True
                     break
            if not found and e['action'] == 'add':
                raise ComplianceError("wanted cron entry not found: '%s' in '%s'"%(s, cron_file))
            if found and e['action'] == 'del':
                raise ComplianceError("unwanted cron entry found: '%s' in '%s'"%(s, cron_file))

    def _del_cron(self, e):
        cron_file = self.get_cron_file(e)

        if cron_file is None:
            raise Unfixable("no crontab usual location found (%s)"%str(self.crontab_locs))

        s = self.format_entry(cron_file, e)

        if not os.path.exists(cron_file):
            return

        new = []
        with open(cron_file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if s == line[:-1]:
                    print "delete entry '%s' from '%s'"%(s, cron_file)
                    continue
                new.append(line)

        if len(new) == 0:
            print 'deleted last entry of %s. delete file too.'%cron_file
            os.unlink(cron_file)
        else:
            with open(cron_file, 'w') as f:
                f.write(''.join(new))
            self.activate_cron(cron_file)

    def _add_cron(self, e):
        cron_file = self.get_cron_file(e)

        if cron_file is None:
            raise Unfixable("no crontab usual location found (%s)"%str(self.crontab_locs))

        s = self.format_entry(cron_file, e)

        new = False
        if os.path.exists(cron_file):
            with open(cron_file, 'r') as f:
                new = f.readlines()
                found = False
                for line in new:
                    if s == line[:-1]:
                        found = True
                        break
                if not found:
                    new.append(s+'\n')
        else:
            new = [s+'\n']

        if not new:
            raise ComplianceError("problem preparing the new crontab '%s'"%cron_file)

        print "add entry '%s' to '%s'"%(s, cron_file)
        with open(cron_file, 'w') as f:
            f.write(''.join(new))
        self.activate_cron(cron_file)

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = CompCron(sys.argv[1])
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

