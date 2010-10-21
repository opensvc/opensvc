from stat import *
import os
import re
import rcExceptions as ex
import xmlrpcClient
from rcGlobalEnv import rcEnv
from rcUtilities import is_exe, justcall

comp_dir = os.path.join(rcEnv.pathvar, 'compliance')

class Module(object):
    pattern = 'S*[0-9]+-*%(name)s'

    def __init__(self, name):
        self.name = name
        self.executable = None

        dl = os.listdir(comp_dir)
        match = []
        for e in dl:
            if re.match(self.pattern%dict(name=name), e) is not None:
                match.append(e)
        if len(match) == 0:
            raise ex.excError('module %s not found in %s'%(name, comp_dir))
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
            if statinfo.st_uid != 0 or statinfo.st_gid != 0:
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

    def log_action(self, out, ret):
        ruleset = ','.join([r['name'] for r in self.ruleset])
        vars = ['run_nodename', 'run_module', 'run_status', 'run_log',
                'run_ruleset']
        vals = [rcEnv.nodename, self.name, str(ret), out, ruleset]
        xmlrpcClient.comp_log_action(vars, vals)

    def action(self, action):
        cmd = [self.executable, action]
        print "[MODULE] %s"%self.name
        (out, err, ret) = justcall(cmd)
        for line in set(err.split('\n'))-set(['']):
            out += "[ERR] %s\n"%line
        print out
        print "[RET] %d"%ret
        self.log_action(out,ret)

    def check(self):
        self.action('check')

    def fix(self):
        self.action('fix')

class Compliance(object):
    def __init__(self, options):
        self.options = options
        self.module_o = {}
        self.module = []

    def __iadd__(self, o):
        self.module_o[o.name] = o
        o.ruleset = self.ruleset
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
            self += Module(module)

        self.ordered_module = self.module_o.keys()
        self.ordered_module.sort(lambda x, y: cmp(self.module_o[x].ordering,
                                                  self.module_o[y].ordering))
        print self

    def __str__(self):
        a = []
        a.append('modules:')
        for m in self.ordered_module:
            a.append(' %0.2d %s'%(self.module_o[m].ordering, m))
        a.append(self.str_ruleset())
        return '\n'.join(a)

    def setup_env(self):
        for rule in self.ruleset:
            var = rule['var']
            var = var.upper().replace('-', '_').replace(' ', '_')
            var = '_'.join(('OSVC_COMP', var))
            os.environ[var] = rule['val']

    def get_moduleset(self):
        moduleset = xmlrpcClient.comp_get_moduleset()
        if moduleset is None:
            raise ex.excError('could not fetch module set')
        return moduleset

    def get_ruleset(self):
        ruleset = xmlrpcClient.comp_get_ruleset()
        if ruleset is None:
            raise ex.excError('could not fetch rule set')
        return ruleset

    def str_ruleset(self):
        a = []
        a.append('rulesets:')
        for rule in self.ruleset:
            if len(rule['filter']) == 0:
                a.append(' %s'%rule['name'])
            else:
                a.append(' %s (%s)'%(rule['name'],rule['filter']))
        a.append('rules:')
        for r in [v for v in os.environ if 'OSVC_COMP_' in v]:
            a.append(' %s=%s'%(r, os.environ[r]))
        return '\n'.join(a)

    def merge_moduleset_modules(self):
        modules = self.get_moduleset_modules(self.moduleset)
        return set(self.module + modules) - set([''])

    def get_moduleset_modules(self, m):
        moduleset = xmlrpcClient.comp_get_moduleset_modules(m)
        if moduleset is None:
            raise ex.excError('could not expand moduleset modules')
        return moduleset

    def do_show_moduleset(self):
        self.moduleset = self.get_moduleset()
        for ms in self.moduleset:
            print ms+':'
            for m in self.get_moduleset_modules(ms):
                print ' %s'%m

    def do_show_ruleset(self):
        self.ruleset = self.get_ruleset()
        self.setup_env()
        print self.str_ruleset()

    def do_checks(self):
        self.init()
        for module in self.ordered_module:
            self.module_o[module].check()

    def do_fix(self):
        self.init()
        for module in self.ordered_module:
            self.module_o[module].fix()

    def do_add_moduleset(self):
        if len(self.options.moduleset) == 0:
            raise ex.excError('no moduleset specified. use --moduleset')
        err = False
        for moduleset in self.options.moduleset.split(','):
            d = xmlrpcClient.comp_add_moduleset(moduleset)
            if not d['status']:
                err = True
            print d['msg']
        if err:
            raise ex.excError()

    def do_del_moduleset(self):
        if len(self.options.moduleset) == 0:
            raise ex.excError('no moduleset specified. use --moduleset')
        err = False
        for moduleset in self.options.moduleset.split(','):
            d = xmlrpcClient.comp_del_moduleset(moduleset)
            if not d['status']:
                err = True
            print d['msg']
        if err:
            raise ex.excError()

    def do_add_ruleset(self):
        if len(self.options.ruleset) == 0:
            raise ex.excError('no ruleset specified. use --ruleset')
        err = False
        for ruleset in self.options.ruleset.split(','):
            d = xmlrpcClient.comp_add_ruleset(ruleset)
            if not d['status']:
                err = True
            print d['msg']
        if err:
            raise ex.excError()

    def do_del_ruleset(self):
        if len(self.options.ruleset) == 0:
            raise ex.excError('no ruleset specified. use --ruleset')
        err = False
        for ruleset in self.options.ruleset.split(','):
            d = xmlrpcClient.comp_del_ruleset(ruleset)
            if not d['status']:
                err = True
            print d['msg']
        if err:
            raise ex.excError()
