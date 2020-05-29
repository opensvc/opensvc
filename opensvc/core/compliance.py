from __future__ import print_function

import os
import sys
import re
import datetime
import json
from stat import *
from subprocess import *

import foreign.six as six
import core.exceptions as ex
from env import Env
from utilities.storage import Storage
from utilities.naming import ANSI_ESCAPE
from utilities.fcache import fcache
from utilities.render.banner import banner
from utilities.render.color import color, colorize, formatter
from utilities.proc import is_exe
from utilities.string import is_string

comp_dir = os.path.join(Env.paths.pathvar, 'compliance')

class Module(object):
    pattern = '^S*[0-9]+-*%(name)s$'

    def __init__(self, name, autofix=False, moduleset=None):
        self.name = name
        self.moduleset = moduleset
        self.executable = None
        self.autofix = autofix
        self.python_link_d = os.path.dirname(sys.executable)
        self.ordering = 0
        self.rset_md5 = ""
        self.ruleset = None
        self.context = None
        self.options = Storage()

        dl = os.listdir(comp_dir)
        match = []
        for e in dl:
            if re.match(self.pattern%dict(name=name), e) is not None:
                match.append(e)
        #if len(match) == 0:
        #    raise ex.InitError('module %s not found in %s'%(name, comp_dir))
        if len(match) > 1:
            raise ex.Error('module %s matches too many entries in %s'%(name,
                              comp_dir))
        if len(match) == 1:
            self.init_module_exe(match[0])

    def init_module_exe(self, fpath):
        base = fpath
        if base[0] == 'S':
            base = base[1:]
        for i, c in enumerate(base):
            if not c.isdigit():
               break
        self.ordering = int(base[0:i])
        regex2 = re.compile("^S*[0-9]+-*", re.UNICODE)
        self.name = regex2.sub("", fpath)

        locations = []
        locations.append(os.path.join(comp_dir, fpath))
        locations.append(os.path.join(locations[0], 'main'))
        locations.append(os.path.join(locations[0], 'scripts', 'main'))

        for loc in locations:
            if not os.path.exists(loc):
                continue
            statinfo = os.stat(loc)
            mode = statinfo[ST_MODE]
            if statinfo.st_uid != 0 or statinfo.st_gid not in (0,2,3,4):
                raise ex.Error('%s is not owned by root. security hazard.'%(loc))
            if not S_ISREG(mode):
                continue
            if not is_exe(loc):
                mode |= S_IXUSR
                os.chmod(loc, mode)
            self.executable = loc

    def __str__(self):
        a = []
        a.append("name: %s"%self.name)
        a.append("ordering: %d"%self.ordering)
        a.append("executable: %s"%self.executable)
        return '\n'.join(a)


    def strip_unprintable(self, s):
        s = ANSI_ESCAPE.sub('', s)
        if six.PY3:
            return s
        else:
            return s.decode('utf8', 'ignore')

    def log_action(self, out, ret, action):
        vals = [Env.nodename,
                self.name,
                str(ret),
                self.strip_unprintable(out),
                action,
                self.rset_md5]
        if self.context.svc:
            vals.append(self.context.svc.path)
        else:
            vals.append("")
        self.context.action_log_vals.append(vals)

    def set_env_path(self):
        if self.python_link_d == sys.path[0]:
            return
        if Env.sysname == "Windows":
            return
        if "PATH" in os.environ:
            os.environ["PATH"] = self.python_link_d + ":" + os.environ["PATH"]
        else:
            os.environ["PATH"] = self.python_link_d
        if Env.paths.pathbin != "/usr/bin":
            os.environ["PATH"] = os.environ["PATH"] + ":" + Env.paths.pathbin

    def set_locale(self):
        """
        Switch to an utf-8 locale
        """
        import locale
        locales = ["C.UTF-8", "en_US.UTF-8"]
        for loc in locales:
            try:
                locale.setlocale(locale.LC_ALL, loc)
                return
            except locale.Error:
                continue

    def reset_env(self):
        self.context.reset_env()

    def setup_env(self):
        os.environ.clear()
        os.environ.update(self.context.env_bkp)
        os.environ.update({
          "PYTHONIOENCODING": "utf-8",
          "OSVC_PYTHON": sys.executable,
          "OSVC_PATH_ETC": Env.paths.pathetc,
          "OSVC_PATH_VAR": Env.paths.pathvar,
          "OSVC_PATH_COMP": Env.paths.pathcomp,
          "OSVC_PATH_TMP": Env.paths.pathtmp,
          "OSVC_PATH_LOG": Env.paths.pathlog,
          "OSVC_NODEMGR": Env.paths.nodemgr,
          "OSVC_SVCMGR": Env.paths.svcmgr,
        })
        self.set_locale()
        self.set_env_path()

        # add services env section keys, with values eval'ed on this node
        if self.context.svc:
            os.environ[self.context.format_rule_var("SVC_NAME")] = self.context.format_rule_val(self.context.svc.name)
            os.environ[self.context.format_rule_var("SVC_PATH")] = self.context.format_rule_val(self.context.svc.path)
            if self.context.svc.namespace:
                os.environ[self.context.format_rule_var("SVC_NAMESPACE")] = self.context.format_rule_val(self.context.svc.namespace)
            for key, val in self.context.svc.env_section_keys_evaluated().items():
                os.environ[self.context.format_rule_var("SVC_CONF_ENV_"+key.upper())] = self.context.format_rule_val(val)

        for rset in self.ruleset.values():
            if (rset["filter"] != "explicit attachment via moduleset" and \
                "matching non-public contextual ruleset shown via moduleset" not in rset["filter"]) or ( \
               self.moduleset in self.context.data["modset_rset_relations"]  and \
               rset['name'] in self.context.data["modset_rset_relations"][self.moduleset]
               ):
                for rule in rset['vars']:
                    var, val, var_class = self.context.parse_rule(rule)
                    os.environ[self.context.format_rule_var(var)] = self.context.format_rule_val(val)


    def action(self, action):
        self.print_bold(banner(self.name))

        if action not in ['check', 'fix', 'fixable', 'env']:
            print('action %s not supported')
            return 1

        if self.options.force:
            # short-circuit all pre and post action
            return self.do_action(action)

        if action == 'fix':
            if self.do_action('check') == 0:
                print('check passed, skip fix')
                return 0
            if self.do_action('fixable') not in (0, 2):
                print('not fixable, skip fix')
                return 1
            self.do_action('fix')
            r = self.do_action('check')
        elif action == 'check':
            r = self.do_action('check')
            if r == 1:
                self.do_action('fixable')
        elif action == 'fixable':
            r = self.do_action('fixable')
        elif action == 'env':
            r = self.do_env()
        return r

    def do_env(self):
        self.setup_env()
        for var in sorted(os.environ):
            print(var, "=", os.environ[var], sep="")
        self.reset_env()
        return 0

    def do_action(self, action):
        self.print_bold("ACTION:   %s"%action)
        if self.executable:
            ret, log = self.do_action_exe(action, [self.executable])
        else:
            ret, log = self.do_action_automodule(action)
        self.print_rcode(ret)
        self.log_action(log, ret, action)
        return ret

    def do_action_automodule(self, action):
        log = ''
        rets = set()

        self.setup_env()
        for rset in self.ruleset.values():
            if rset["name"] != self.moduleset:
                continue
            if "via moduleset" not in rset["filter"]:
                continue
            for rule in sorted(rset['vars'], key=lambda x: x[0]):
                var, val, var_class = self.context.parse_rule(rule)
                if var_class == "raw":
                    continue
                obj = self.get_obj(var_class)
                if obj is None:
                    err = color.RED + 'ERR: ' + color.END + "no compliance object found to handle class '%s' for rule '%s'" % (var_class, var)
                    log += err + "\n"
                    print(err, file=sys.stderr)
                    continue
                _ret, _log = self.do_action_exe(action, Env.python_cmd + [obj, self.context.format_rule_var(var)])
                rets.add(_ret)
                log += _log
                if action == "fix" and _ret not in (0, 2):
                    # stop at frist error in a 'fix' action
                    break

        self.reset_env()
        if rets == set([0]) or rets == set():
            ret = 0
        elif rets == set([0, 2]):
            ret = 0
        elif rets == set([2]):
            ret = 2
        else:
            ret = 1
        return ret, log

    def get_obj(self, var_class):
        import glob
        try:
            return glob.glob(os.path.join(comp_dir, "*", var_class+".py"))[0]
        except IndexError:
            return None

    def do_action_exe(self, action, executable):
        cmd = executable + [action]
        log = ''

        import tempfile
        import time
        fo = tempfile.NamedTemporaryFile()
        fe = tempfile.NamedTemporaryFile()
        _fo = None
        _fe = None

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
            _line = color.RED + 'ERR: ' + color.END + line
            line = 'ERR: '+line
            sys.stdout.write(_line)
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
            self.setup_env()
            p = Popen(cmd, stdout=fo, stderr=fe, env=os.environ)
            _fo = open(fo.name, 'r')
            _fe = open(fe.name, 'r')
            while True:
                time.sleep(0.1)
                log = poll_pipes(log)
                if p.poll() != None:
                    log = poll_pipes(log)
                    break
        except OSError as e:
            if _fo is not None:
                _fo.close()
            if _fe is not None:
                _fe.close()
            fo.close()
            fe.close()
            if e.errno == 2:
                raise ex.Error("%s execution error (File not found or bad interpreter)"%cmd[0])
            elif e.errno == 8:
                raise ex.Error("%s execution error (Exec format error)"%cmd[0])
            else:
                raise
        fo.close()
        fe.close()
        _fo.close()
        _fe.close()
        self.reset_env()
        return p.returncode, log

    def print_bold(self, s):
        print(colorize(s, color.BOLD))

    def print_rcode(self, r):
        buff = "STATUS:   "
        if r == 1:
            buff += colorize("nok", color.RED)
        elif r == 0:
            buff += colorize("ok", color.GREEN)
        elif r == 2:
            buff += "n/a"
        else:
            buff += "%d" % r
        print(buff)

    def env(self):
        return self.action('env')

    def check(self):
        return self.action('check')

    def fix(self):
        return self.action('fix')

    def fixable(self):
        return self.action('fixable')

class Compliance(object):
    def __init__(self, o=None):
        if hasattr(o, "path"):
            self.svc = o
            self.node = o.node
        else:
            self.svc = None
            self.node = o
        self.options = o.options
        self.module_o = {}
        self.module = []
        self.updatecomp = False
        self.moduleset = None
        self.data = None
        self.action_log_vals = []
        self.action_log_vars = [
          'run_nodename',
          'run_module',
          'run_status',
          'run_log',
          'run_action',
          'rset_md5',
          'run_svcname']
        self.env_bkp = os.environ.copy()
        self.ordered_module = []

    def set_rset_md5(self):
        self.rset_md5 = ""
        rset = self.ruleset.get("osvc_collector")
        if rset is None:
            return
        for rule in rset["vars"]:
            var, val, var_class = self.parse_rule(rule)
            if var == "ruleset_md5":
                self.rset_md5 = val
                break

    def parse_rule(self, var):
        if len(var) == 2:
            return var[0], var[1], "raw"
        else:
            return var

    def setup_env(self):
        for rset in self.ruleset.values():
            for rule in rset['vars']:
                var, val, var_class = self.parse_rule(rule)
                os.environ[self.format_rule_var(var)] = self.format_rule_val(val)

    def reset_env(self):
        os.environ.clear()
        os.environ.update(self.env_bkp)

    def compliance_auto(self):
        if self.updatecomp and self.svc is None:
            self.node.updatecomp()
        self.do_auto()

    def compliance_env(self):
        self.do_run('env')

    def compliance_check(self):
        self.do_checks()

    def __iadd__(self, o):
        self.module_o[o.name] = o
        o.ruleset = self.ruleset
        o.options = self.options
        o.collector = self.node.collector
        o.context = self
        o.rset_md5 = self.rset_md5
        return self

    def print_bold(self, s):
        print(colorize(s, color.BOLD))

    def expand_modulesets(self, modulesets):
        l = []

        def recurse(ms):
            l.append(ms)
            if ms not in self.data["modset_relations"]:
                return
            for _ms in self.data["modset_relations"][ms]:
                recurse(_ms)

        for ms in modulesets:
            recurse(ms)

        return l

    def init(self):
        if self.options.moduleset != "" and self.options.module != "":
            raise ex.Error('--moduleset and --module are exclusive')

        if len(self.options.moduleset) != "" and \
           hasattr(self.options, "attach") and self.options.attach:
            self._compliance_attach_moduleset(self.options.moduleset.split(','))

        if self.data is None:
            try:
                self.data = self.get_comp_data()
            except Exception as e:
                raise ex.Error(str(e))
            if self.data is None:
                raise ex.Error("could not fetch compliance data from the collector")
            if "ret" in self.data and self.data["ret"] == 1:
                if "msg" in self.data:
                    raise ex.Error(self.data["msg"])
                raise ex.Error("could not fetch compliance data from the collector")
            modulesets = []
            if self.options.moduleset != "":
                # purge unspecified modulesets
                modulesets = self.options.moduleset.split(',')
                modulesets = self.expand_modulesets(modulesets)
                for ms in list(self.data["modulesets"].keys()):
                    if ms not in modulesets:
                        del(self.data["modulesets"][ms])
            elif self.options.module != "":
                # purge unspecified modules
                modules = self.options.module.split(',')
                for ms, data in self.data["modulesets"].items():
                    n = len(data)
                    for i in sorted(range(n), reverse=True):
                        module, autofix = data[i]
                        if module not in modules:
                            del(self.data["modulesets"][ms][i])
                for module in modules:
                    in_modsets = []
                    for ms, data in self.data["modulesets"].items():
                        for _module, autofix in data:
                            if module == _module:
                               in_modsets.append(ms)
                    if len(in_modsets) == 0:
                        print("module %s not found in any attached moduleset" % module)
                    elif len(in_modsets) > 1:
                        raise ex.Error("module %s found in multiple attached moduleset (%s). Use --moduleset instead of --module to clear the ambiguity" % (module, ', '.join(in_modsets)))

        self.module = self.merge_moduleset_modules()
        self.ruleset = self.data['rulesets']
        self.set_rset_md5()

        if not os.path.exists(comp_dir):
            os.makedirs(comp_dir, 0o755)

        for module, autofix, moduleset in self.module:
            try:
                self += Module(module, autofix, moduleset)
            except ex.InitError as e:
                print(e, file=sys.stderr)

        self.ordered_module = list(self.module_o.keys())
        self.ordered_module.sort(key=lambda x: self.module_o[x].ordering)

    def __str__(self):
        print(banner('run context'))
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
        if is_string(val):
            try:
                tmp = json.loads(val)
                val = json.dumps(tmp)
            except Exception as e:
                pass
            if six.PY2:
                val = val.encode("utf-8")
        else:
            val = str(val)
        return val

    def get_moduleset(self):
        if self.svc:
            moduleset = self.node.collector.call('comp_get_svc_data_moduleset', self.svc.path)
        else:
            moduleset = self.node.collector.call('comp_get_data_moduleset')
        if moduleset is None:
            raise ex.Error('could not fetch moduleset')
        return moduleset

    def get_ruleset(self):
        if hasattr(self.options, 'ruleset') and \
           len(self.options.ruleset) > 0:
            return self.get_ruleset_md5(self.options.ruleset)
        return self.get_current_ruleset()

    def get_current_ruleset(self):
        if self.svc:
            ruleset = self.node.collector.call('comp_get_svc_ruleset', self.svc.path)
        else:
            ruleset = self.node.collector.call('comp_get_ruleset')
        if ruleset is None:
            raise ex.Error('could not fetch ruleset')
        return ruleset

    def get_ruleset_md5(self, rset_md5):
        ruleset = self.node.collector.call('comp_get_ruleset_md5', rset_md5)
        if ruleset is None:
            raise ex.Error('could not fetch ruleset')
        return ruleset

    def str_ruleset(self):
        a = []
        a.append('rules:')
        for rset in self.ruleset.values():
            if len(rset['filter']) == 0:
                a.append(' %s'%rset['name'])
            else:
                a.append(' %s (%s)'%(rset['name'],rset['filter']))
            for rule in rset['vars']:
                var, val, var_class = self.parse_rule(rule)
                val = self.format_rule_val(val)
                if ' ' in val:
                    val = repr(val)
                a.append('  %s=%s'%(self.format_rule_var(var), val))
        return '\n'.join(a)

    @fcache
    def get_comp_data(self):
        if self.svc:
            return self.node.collector.call('comp_get_svc_data',
                                            self.svc.path,
                                            modulesets=self.options.moduleset.split(','))
        else:
            return self.node.collector.call('comp_get_data',
                                            modulesets=self.options.moduleset.split(','))

    def merge_moduleset_modules(self):
        l = []
        for ms, data in self.data['modulesets'].items():
            for module, autofix in data:
                if (module, autofix) not in l:
                    l.append((module, autofix, ms))
                elif autofix and (module, False, ms) in l:
                    l.remove((module, False, ms))
                    l.append((module, True, ms))
        return l

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

        self.print_bold(banner("digest"))
        print("%d n/a%s"%(n_na, modules(na)))
        print("%d passed%s"%(n_passed, modules(passed)))
        print("%d error%s%s"%(n_errors, _s(n_errors), modules(errors)))

        if len(errors) > 0:
            return 1
        return 0

    def compliance_show_moduleset(self):
        def recurse(ms, depth=0):
            prefix=" "*depth
            print(prefix+ms+':')
            if ms not in data["modulesets"]:
                print(prefix+" (no modules)")
                return
            for module, autofix in data["modulesets"][ms]:
                if autofix:
                    s = " (autofix)"
                else:
                    s = ""
                print(prefix+' %s%s' % (module, s))
            if ms in data["modset_relations"]:
                for _ms in data["modset_relations"][ms]:
                    recurse(_ms, depth+1)

        try:
            data = self.get_moduleset()
        except Exception as e:
            print(e, file=sys.stderr)
            return 1
        if "ret" in data and data["ret"] == 1:
            if "msg" in data:
                print(data["msg"], file=sys.stderr)
            return 1
        if "root_modulesets" not in data:
            print("(none)")
            return 0
        for ms in data["root_modulesets"]:
            recurse(ms)

    def compliance_show_ruleset(self):
        self.ruleset = self.get_ruleset()
        print(self.str_ruleset())

    def do_run(self, action):
        err = {}
        self.init()
        start = datetime.datetime.now()
        for module in self.ordered_module:
            _action = action
            if action == "auto":
                if self.module_o[module].autofix:
                    _action = "fix"
                else:
                    _action = "check"
            err[module] = getattr(self.module_o[module], _action)()
        if action == "env":
            return 0
        r = self.digest_errors(err)
        end = datetime.datetime.now()
        print("total duration: %s"%str(end-start))
        self.node.collector.call('comp_log_actions', self.action_log_vars, self.action_log_vals)
        return r

    def do_auto(self):
        return self.do_run('auto')

    def do_checks(self):
        return self.do_run('check')

    def compliance_fix(self):
        return self.do_run('fix')

    def compliance_fixable(self):
        return self.do_run('fixable')

    def compliance_detach(self):
        did_something = False
        if hasattr(self.options, 'moduleset') and \
           len(self.options.moduleset) > 0:
            did_something = True
            self._compliance_detach_moduleset(self.options.moduleset.split(','))
        if hasattr(self.options, 'ruleset') and \
           len(self.options.ruleset) > 0:
            did_something = True
            self._compliance_detach_ruleset(self.options.ruleset.split(','))
        if not did_something:
            raise ex.Error('no moduleset nor ruleset specified. use --moduleset and/or --ruleset')

    def compliance_attach(self):
        did_something = False
        if hasattr(self.options, 'moduleset') and \
           len(self.options.moduleset) > 0:
            did_something = True
            self._compliance_attach_moduleset(self.options.moduleset.split(','))
        if hasattr(self.options, 'ruleset') and \
           len(self.options.ruleset) > 0:
            did_something = True
            self._compliance_attach_ruleset(self.options.ruleset.split(','))
        if not did_something:
            raise ex.Error('no moduleset nor ruleset specified. use --moduleset and/or --ruleset')

    def _compliance_attach_moduleset(self, modulesets):
        err = False
        for moduleset in modulesets:
            if self.svc:
                d = self.node.collector.call('comp_attach_svc_moduleset', self.svc.path, moduleset)
            else:
                d = self.node.collector.call('comp_attach_moduleset', moduleset)
            if d is None:
                print("Failed to attach '%s' moduleset. The collector may not be reachable." % moduleset, file=sys.stderr)
                err = True
                continue
            if not d.get("status", True) or d.get("ret"):
                err = True
            print(d['msg'])
        if err:
            raise ex.Error()

    def _compliance_detach_moduleset(self, modulesets):
        err = False
        for moduleset in modulesets:
            if self.svc:
                d = self.node.collector.call('comp_detach_svc_moduleset', self.svc.path, moduleset)
            else:
                d = self.node.collector.call('comp_detach_moduleset', moduleset)
            if d is None:
                print("Failed to detach '%s' moduleset. The collector may not be reachable." % moduleset, file=sys.stderr)
                err = True
                continue
            if not d.get("status", True) or d.get("ret"):
                err = True
            print(d['msg'])
        if err:
            raise ex.Error()

    def _compliance_attach_ruleset(self, rulesets):
        err = False
        for ruleset in rulesets:
            if self.svc:
                d = self.node.collector.call('comp_attach_svc_ruleset', self.svc.path, ruleset)
            else:
                d = self.node.collector.call('comp_attach_ruleset', ruleset)
            if d is None:
                print("Failed to attach '%s' ruleset. The collector may not be reachable." % ruleset, file=sys.stderr)
                err = True
                continue
            if not d.get("status", True) or d.get("ret"):
                err = True
            print(d['msg'])
        if err:
            raise ex.Error()

    def _compliance_detach_ruleset(self, rulesets):
        err = False
        for ruleset in rulesets:
            if self.svc:
                d = self.node.collector.call('comp_detach_svc_ruleset', self.svc.path, ruleset)
            else:
                d = self.node.collector.call('comp_detach_ruleset', ruleset)
            if d is None:
                print("Failed to detach '%s' ruleset. The collector may not be reachable." % ruleset, file=sys.stderr)
                err = True
                continue
            if not d.get("status", True) or d.get("ret"):
                err = True
            print(d['msg'])
        if err:
            raise ex.Error()

    @formatter
    def compliance_show_status(self):
        return self._compliance_show_status()

    def _compliance_show_status(self):
        args = ['comp_show_status']
        if self.svc:
           args.append(self.svc.path)
        else:
           args.append('')
        if hasattr(self.options, 'module') and \
           len(self.options.module) > 0:
            args.append(self.options.module)
        l = self.node.collector.call(*args)
        if l is None:
            return
        return l

    def compliance_list_ruleset(self):
        if not hasattr(self.options, 'ruleset') or \
           len(self.options.ruleset) == 0:
            l = self.node.collector.call('comp_list_ruleset')
        else:
            l = self.node.collector.call('comp_list_ruleset', self.options.ruleset)
        if l is None:
            return
        print('\n'.join(l))

    def compliance_list_moduleset(self):
        if not hasattr(self.options, 'moduleset') or \
           len(self.options.moduleset) == 0:
            l = self.node.collector.call('comp_list_moduleset')
        else:
            l = self.node.collector.call('comp_list_moduleset', self.options.moduleset)
        if l is None:
            return
        if isinstance(l, dict) and l.get("ret", 0) != 0:
            raise ex.Error(l.get("msg", ""))
        print('\n'.join(l))

    def compliance_list_module(self):
        import glob
        regex2 = re.compile("^S*[0-9]+-*", re.UNICODE)
        for path in glob.glob(os.path.join(comp_dir, '*')):
            name = regex2.sub("", os.path.basename(path))
            try:
                m = Module(name)
                print(m.name)
            except:
                continue


