from stat import *
import os
import sys
import re
import datetime
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import is_exe, justcall, banner
from subprocess import *

comp_dir = os.path.join(rcEnv.pathvar, 'compliance')

# ex: \x1b[37;44m\x1b[1mContact List\x1b[0m\n
regex = re.compile("\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[m|K|G]", re.UNICODE)

class Module(object):
    pattern = '^S*[0-9]+-*%(name)s$'

    def __init__(self, name):
        self.name = name
        self.executable = None

        dl = os.listdir(comp_dir)
        match = []
        for e in dl:
            if re.match(self.pattern%dict(name=name), e) is not None:
                match.append(e)
        if len(match) == 0:
            raise ex.excInitError('module %s not found in %s'%(name, comp_dir))
        if len(match) > 1:
            raise ex.excError('module %s matches too many entries in %s'%(name,
                              comp_dir))
        base = match[0]
        if base[0] == 'S':
            base == base[1:]
        for i, c in enumerate(base):
            if not c.isdigit():
               break
        self.ordering = int(base[0:i])

        locations = []
        locations.append(os.path.join(comp_dir, match[0]))
        locations.append(os.path.join(locations[0], 'main'))
        locations.append(os.path.join(locations[0], 'scripts', 'main'))

        for loc in locations:
            if not os.path.exists(loc):
                continue
            statinfo = os.stat(loc)
            mode = statinfo[ST_MODE]
            if statinfo.st_uid != 0 or statinfo.st_gid not in (0,2,3,4):
                raise ex.excError('%s is not owned by root. security hazard.'%(loc))
            if not S_ISREG(mode):
                continue
            if not is_exe(loc):
                mode |= S_IXUSR
                os.chmod(loc, mode)
            self.executable = loc
        if self.executable is None:
            raise ex.excError('executable not found for module %s'%(name))

    def __str__(self):
        a = []
        a.append("name: %s"%self.name)
        a.append("ordering: %d"%self.ordering)
        a.append("executable: %s"%self.executable)
        return '\n'.join(a)


    def strip_unprintable(self, s):
        return regex.sub('', s)

    def log_action(self, out, ret, action):
        ruleset = ','.join(self.ruleset)
        vars = ['run_nodename', 'run_module', 'run_status', 'run_log',
                'run_ruleset', 'run_action']
        vals = [rcEnv.nodename,
                self.name,
                str(ret),
                self.strip_unprintable(out),
                ruleset,
                action]
        if self.svcname is not None:
            vars.append('run_svcname')
            vals.append(self.svcname)
        self.collector.call('comp_log_action', vars, vals, sync=False)

    def action(self, action):
        print banner(self.name)

        if action not in ['check', 'fix', 'fixable']:
            print 'action %s not supported'
            return 1

        if self.options.force:
            # short-circuit all pre and post action
            return self.do_action(action)

        if action == 'fix':
            if self.do_action('check') == 0:
                print 'check passed, skip fix'
                return 0
            if self.do_action('fixable') not in (0, 2):
                print 'not fixable, skip fix'
                return 1
            self.do_action('fix')
            r = self.do_action('check')
        elif action == 'check':
            r = self.do_action('check')
            if r == 1:
                self.do_action('fixable')
        elif action == 'fixable':
            r = self.do_action('fixable')
        return r

    def do_action(self, action):
        start = datetime.datetime.now()
        cmd = [self.executable, action]
        log = ''
        print "ACTION:   %s"%action
        print "START:    %s"%str(start)
        print "COMMAND:  %s"%' '.join(cmd)
        print "LOG:"

        import tempfile
        import time
        fo = tempfile.NamedTemporaryFile()
        fe = tempfile.NamedTemporaryFile()

        def poll_out():
            fop = _fo.tell()
            line = _fo.readline()
            if not line:
                _fo.seek(fop)
                return None
            sys.stdout.write(line)
            sys.stdout.flush()
            return line

        def poll_err():
            fep = _fe.tell()
            line = _fe.readline()
            if not line:
                _fe.seek(fep)
                return None
            line = 'ERR: '+line
            sys.stdout.write(line)
            sys.stdout.flush()
            return line

        def poll_pipes(log):
            i = 0
            while True:
                o = poll_out()
                e = poll_err()
                if o is not None:
                    log += o
                if e is not None:
                    log += e
                if o is None and e is None:
                    break
            return log

        try:
            p = Popen(cmd, stdout=fo, stderr=fe)
            _fo = open(fo.name, 'r')
            _fe = open(fe.name, 'r')
            while True:
                time.sleep(0.1)
                log = poll_pipes(log)
                if p.poll() != None:
                    log = poll_pipes(log)
                    break
        except OSError, e:
            _fo.close()
            _fe.close()
            fo.close()
            fe.close()
            if e.errno == 8:
                raise ex.excError("%s execution error (Exec format error)"%self.executable)
            else:
                raise
        fo.close()
        fe.close()
        _fo.close()
        _fe.close()
        end = datetime.datetime.now()
        print "RCODE:    %d"%p.returncode
        print "DURATION: %s"%str(end-start)
        self.log_action(log, p.returncode, action)
        return p.returncode

    def check(self):
        return self.action('check')

    def fix(self):
        return self.action('fix')

    def fixable(self):
        return self.action('fixable')

class Compliance(object):
    def __init__(self, skip_action=None, options=None, collector=None, svcname=None):
        self.skip_action = skip_action
        self.options = options
        self.collector = collector
        self.svcname = svcname
        self.options = options
        self.module_o = {}
        self.module = []

    def compliance_check(self):
        flag = "last_comp_check"
        if self.svcname is not None:
            flag = '.'.join((flag, self.svcname))
        if self.skip_action is not None and \
           self.skip_action("compliance", 'comp_check_interval', flag,
                            period_option='comp_check_period',
                            days_option='comp_check_days',
                            force=self.options.force):
            return
        self.do_checks()

    def __iadd__(self, o):
        self.module_o[o.name] = o
        o.svcname = self.svcname
        o.ruleset = self.ruleset
        o.options = self.options
        o.collector = self.collector
        return self

    def init(self):
        if self.options.moduleset != "" and self.options.module != "":
            raise ex.excError('--moduleset and --module are exclusive')

        if self.options.moduleset == "" and self.options.module == "":
            self.moduleset = self.get_moduleset()
        else:
            self.moduleset = self.options.moduleset.split(',')
            self.module = self.options.module.split(',')
        self.module = self.merge_moduleset_modules()
        self.ruleset = self.get_ruleset()
        self.setup_env()

        if not os.path.exists(comp_dir):
            os.makedirs(comp_dir, 0755)
            raise ex.excError('modules [%s] are not present in %s'%(
                               ','.join(self.module), comp_dir))

        for module in self.module:
            try:
                self += Module(module)
            except ex.excInitError, e:
                print >>sys.stderr, e

        self.ordered_module = self.module_o.keys()
        self.ordered_module.sort(lambda x, y: cmp(self.module_o[x].ordering,
                                                  self.module_o[y].ordering))
        print self

    def __str__(self):
        print banner('run context')
        a = []
        a.append('modules:')
        for m in self.ordered_module:
            a.append(' %0.2d %s'%(self.module_o[m].ordering, m))
        a.append(self.str_ruleset())
        return '\n'.join(a)

    def format_rule_var(self, var):
        var = var.upper().replace('-', '_').replace(' ', '_').replace('.','_')
        var = '_'.join(('OSVC_COMP', var))
        return var

    def format_rule_val(self, val):
        if isinstance(val, unicode):
            val = repr(val).strip("'")
        else:
            val = str(val)
        illegal_chars = """`;$"""
        for c in illegal_chars:
            if c in val:
                print "illegal char %s in variable value: %s"%(c,val)
                return "suppressed"
        return val

    def setup_env(self):
        for rule in self.ruleset.values():
            for var, val in rule['vars']:
                os.environ[self.format_rule_var(var)] = self.format_rule_val(val)

    def get_moduleset(self):
        if self.svcname is not None:
            moduleset = self.collector.call('comp_get_svc_moduleset', self.svcname)
        else:
            moduleset = self.collector.call('comp_get_moduleset')
        if moduleset is None:
            raise ex.excError('could not fetch moduleset')
        return moduleset

    def get_ruleset(self):
        if hasattr(self.options, 'ruleset_date') and \
           len(self.options.ruleset_date) > 0:
            return self.get_dated_ruleset(self.options.ruleset_date)
        return self.get_current_ruleset()

    def get_current_ruleset(self):
        if self.svcname is not None:
            ruleset = self.collector.call('comp_get_svc_ruleset', self.svcname)
        else:
            ruleset = self.collector.call('comp_get_ruleset')
        if ruleset is None:
            raise ex.excError('could not fetch ruleset')
        return ruleset

    def get_dated_ruleset(self, date):
        if self.svcname is not None:
            ruleset = self.collector.call('comp_get_dated_ruleset', self.options.ruleset_date)
        else:
            ruleset = self.collector.call('comp_get_dated_svc_ruleset', self.svcname, self.options.ruleset_date)
        if ruleset is None:
            raise ex.excError('could not fetch ruleset')
        return ruleset

    def str_ruleset(self):
        a = []
        a.append('rules:')
        for rule in self.ruleset.values():
            if len(rule['filter']) == 0:
                a.append(' %s'%rule['name'])
            else:
                a.append(' %s (%s)'%(rule['name'],rule['filter']))
            for var, val in rule['vars']:
                val = self.format_rule_val(val)
                if ' ' in val:
                    val = repr(val)
                a.append('  %s=%s'%(self.format_rule_var(var), val))
        return '\n'.join(a)

    def merge_moduleset_modules(self):
        modules = self.get_moduleset_modules(self.moduleset)
        return set(self.module + modules) - set([''])

    def get_moduleset_modules(self, m):
        moduleset = self.collector.call('comp_get_moduleset_modules', m)
        if moduleset is None:
            raise ex.excError('could not expand moduleset modules')
        return moduleset

    def digest_errors(self, err):
        passed = [m for m in err if err[m] == 0]
        errors = [m for m in err if err[m] == 1]
        na = [m for m in err if err[m] == 2]

        n_passed = len(passed)
        n_errors = len(errors)
        n_na = len(na)

        def _s(n):
            if n > 1:
                return 's'
            else:
                return ''

        def modules(l):
            if len(l) == 0:
                return ''
            return '\n%s'%'\n'.join(map(lambda x: ' '+x, l))

        print banner("digest")
        print "%d n/a%s"%(n_na, modules(na))
        print "%d passed%s"%(n_passed, modules(passed))
        print "%d error%s%s"%(n_errors, _s(n_errors), modules(errors))

        if len(errors) > 0:
            return 1
        return 0

    def compliance_show_moduleset(self):
        self.moduleset = self.get_moduleset()
        for ms in self.moduleset:
            print ms+':'
            for m in self.get_moduleset_modules(ms):
                print ' %s'%m

    def compliance_show_ruleset(self):
        self.ruleset = self.get_ruleset()
        self.setup_env()
        print self.str_ruleset()

    def do_run(self, action):
        err = {}
        self.init()
        start = datetime.datetime.now()
        for module in self.ordered_module:
            err[module] = getattr(self.module_o[module], action)()
        r = self.digest_errors(err)
        end = datetime.datetime.now()
        print "total duration: %s"%str(end-start)
        return r

    def do_checks(self):
        return self.do_run('check')

    def compliance_fix(self):
        return self.do_run('fix')

    def compliance_fixable(self):
        return self.do_run('fixable')

    def compliance_attach_moduleset(self):
        if not hasattr(self.options, 'moduleset') or \
           len(self.options.moduleset) == 0:
            raise ex.excError('no moduleset specified. use --moduleset')
        err = False
        for moduleset in self.options.moduleset.split(','):
            if self.svcname is not None:
                d = self.collector.call('comp_attach_svc_moduleset', self.svcname, moduleset)
            else:
                d = self.collector.call('comp_attach_moduleset', moduleset)
            if not d['status']:
                err = True
            print d['msg']
        if err:
            raise ex.excError()

    def compliance_detach_moduleset(self):
        if not hasattr(self.options, 'moduleset') or \
           len(self.options.moduleset) == 0:
            raise ex.excError('no moduleset specified. use --moduleset')
        err = False
        for moduleset in self.options.moduleset.split(','):
            if self.svcname is not None:
                d = self.collector.call('comp_detach_svc_moduleset', self.svcname, moduleset)
            else:
                d = self.collector.call('comp_detach_moduleset', moduleset)
            if not d['status']:
                err = True
            print d['msg']
        if err:
            raise ex.excError()

    def compliance_attach_ruleset(self):
        if not hasattr(self.options, 'ruleset') or \
           len(self.options.ruleset) == 0:
            raise ex.excError('no ruleset specified. use --ruleset')
        err = False
        for ruleset in self.options.ruleset.split(','):
            d = self.collector.call('comp_attach_ruleset', ruleset)
            if not d['status']:
                err = True
            print d['msg']
        if err:
            raise ex.excError()

    def compliance_detach_ruleset(self):
        if not hasattr(self.options, 'ruleset') or \
           len(self.options.ruleset) == 0:
            raise ex.excError('no ruleset specified. use --ruleset')
        err = False
        for ruleset in self.options.ruleset.split(','):
            d = self.collector.call('comp_detach_ruleset', ruleset)
            if not d['status']:
                err = True
            print d['msg']
        if err:
            raise ex.excError()

    def compliance_list_ruleset(self):
        if not hasattr(self.options, 'ruleset') or \
           len(self.options.ruleset) == 0:
            l = self.collector.call('comp_list_ruleset')
        else:
            l = self.collector.call('comp_list_ruleset', self.options.ruleset)
        print '\n'.join(l)

    def compliance_list_moduleset(self):
        if not hasattr(self.options, 'moduleset') or \
           len(self.options.moduleset) == 0:
            l = self.collector.call('comp_list_moduleset')
        else:
            l = self.collector.call('comp_list_moduleset', self.options.moduleset)
        if l is None:
            return
        print '\n'.join(l)


