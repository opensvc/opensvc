from __future__ import print_function

import ast
import datetime
import json
import glob
import importlib
import locale
import logging
import operator as op
import os
import re
import socket
import select
import shlex
import sys
import time
from functools import wraps
from subprocess import Popen, PIPE
from itertools import chain

import six
import lock
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from contexts import want_context

VALID_NAME_RFC952_NO_DOT = (r"^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]))*"
                            r"([A-Za-z]|[A-Za-z][A-Za-z0-9-]*[A-Za-z0-9])$")
VALID_NAME_RFC952 = (r"^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9])\.)*"
                     r"([A-Za-z]|[A-Za-z][A-Za-z0-9-]*[A-Za-z0-9])$")
GLOB_ROOT_SVC_CONF = os.path.join(rcEnv.paths.pathetc, "*.conf")
GLOB_ROOT_VOL_CONF = os.path.join(rcEnv.paths.pathetc, "vol", "*.conf")
GLOB_ROOT_CFG_CONF = os.path.join(rcEnv.paths.pathetc, "cfg", "*.conf")
GLOB_ROOT_SEC_CONF = os.path.join(rcEnv.paths.pathetc, "sec", "*.conf")
GLOB_ROOT_USR_CONF = os.path.join(rcEnv.paths.pathetc, "usr", "*.conf")
GLOB_CONF_NS = os.path.join(rcEnv.paths.pathetcns, "*", "*", "*.conf")
GLOB_CONF_NS_ONE = os.path.join(rcEnv.paths.pathetcns, "%s", "*", "*.conf")

ANSI_ESCAPE = re.compile(r"\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[mHJKG]", re.UNICODE)
ANSI_ESCAPE_B = re.compile(br"\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[mHJKG]")

# Os where os.access is invalid
OS_WITHOUT_OS_ACCESS = ['SunOS']

# supported operators in arithmetic expressions
operators = {
    ast.Add: op.add,
    ast.Sub: op.sub,
    ast.Mult: op.mul,
    ast.Div: op.truediv,
    ast.Pow: op.pow,
    ast.BitOr: op.or_,
    ast.BitAnd: op.and_,
    ast.BitXor: op.xor,
    ast.USub: op.neg,
    ast.FloorDiv: op.floordiv,
    ast.Mod: op.mod,
    ast.Not: op.not_,
    ast.Eq: op.eq,
    ast.NotEq: op.ne,
    ast.Lt: op.lt,
    ast.LtE: op.le,
    ast.Gt: op.gt,
    ast.GtE: op.ge,
    ast.In: op.contains,
}


def eval_expr(expr):
    """ arithmetic expressions evaluator
    """

    def eval_(node):
        _safe_names = {'None': None, 'True': True, 'False': False}
        if isinstance(node, ast.Num):  # <number>
            return node.n
        elif isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Name):
            if node.id in _safe_names:
                return _safe_names[node.id]
            return node.id
        elif isinstance(node, ast.Tuple):
            return tuple(node.elts)
        elif isinstance(node, ast.BinOp):  # <left> <operator> <right>
            return operators[type(node.op)](eval_(node.left), eval_(node.right))
        elif isinstance(node, ast.UnaryOp):  # <operator> <operand> e.g., -1
            return operators[type(node.op)](eval_(node.operand))
        elif isinstance(node, ast.BoolOp):  # Boolean operator: either "and" or "or" with two or more values
            if type(node.op) == ast.And:
                return all(eval_(val) for val in node.values)
            else:  # Or:
                for val in node.values:
                    result = eval_(val)
                    if result:
                        return result
                    return result  # or returns the final value even if it's falsy
        elif isinstance(node, ast.Compare):  # A comparison expression, e.g. "3 > 2" or "5 < x < 10"
            left = eval_(node.left)
            for comparison_op, right_expr in zip(node.ops, node.comparators):
                right = eval_(right_expr)
                if type(comparison_op) == ast.In:
                    if isinstance(right, tuple):
                        if not any(q.id == left for q in right if isinstance(q, ast.Name)):
                            return False
                    else:
                        if not operators[type(comparison_op)](right, left):
                            return False
                else:
                    if not operators[type(comparison_op)](left, right):
                        return False
                left = right
                return True
        elif isinstance(node, ast.Attribute):
            raise TypeError("strings with dots need quoting")
        elif hasattr(ast, "NameConstant") and isinstance(node, getattr(ast, "NameConstant")):
            return node.value
        else:
            raise TypeError("unsupported node type %s" % type(node))

    return eval_(ast.parse(expr, mode='eval').body)


PROTECTED_DIRS = [
    '/',
    '/bin',
    '/boot',
    '/dev',
    '/dev/pts',
    '/dev/shm',
    '/home',
    '/opt',
    '/proc',
    '/sys',
    '/tmp',
    '/usr',
    '/var',
]

if os.name == 'nt':
    close_fds = False
else:
    close_fds = True


#############################################################################
#
# Cached functions
#
#############################################################################

def fcache(fn):
    """
    A decorator for caching the result of a function
    """
    attr_name = '_fcache_' + fn.__name__

    def _fcache(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _fcache


def fcache_initialized(self, attr):
    """
    Return True if the function has already been cached
    """
    attr_name = '_fcache_' + attr
    if hasattr(self, attr_name):
        return True
    return False


def unset_fcache(self, attr):
    """
    Unset <attr> function cache
    """
    attr_name = '_fcache_' + attr
    if hasattr(self, attr_name):
        delattr(self, attr_name)


#############################################################################
#
# Lazy properties
#
#############################################################################
def lazy(fn):
    """
    A decorator for on-demand initialization of a property
    """
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazyprop(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _lazyprop


def lazy_initialized(self, attr):
    """
    Return True if the lazy property has been initialized
    """
    attr_name = '_lazy_' + attr
    if hasattr(self, attr_name):
        return True
    return False


def set_lazy(self, attr, value):
    """
    Set a <value> as the <self> object lazy property hidden property value
    """
    attr_name = '_lazy_' + attr
    setattr(self, attr_name, value)


def unset_all_lazy(self):
    """
    Unset all lazy property hidden property, iow flush the cache
    """
    for attr in [attr for attr in self.__dict__]:
        if attr.startswith("_lazy_"):
            delattr(self, attr)


def unset_lazy(self, attr):
    """
    Unset <attr> lazy property hidden property, iow flush the cache
    """
    attr_name = '_lazy_' + attr
    if hasattr(self, attr_name):
        delattr(self, attr_name)


def bencode(buff):
    """
    Try a bytes cast, which only work in python3.
    """
    try:
        return bytes(buff, "utf-8")
    except TypeError:
        return buff


def bdecode(buff):
    """
    On python, convert bytes to string using utf-8 and ascii as a fallback
    """
    if buff is None:
        return buff
    if six.PY2:
        return buff
    if type(buff) == str:
        return buff
    return buff.decode("utf-8", errors="ignore")


def is_string(s):
    """
    python[23] compatible string-type test
    """
    if isinstance(s, six.string_types):
        return True
    return False


def driver_import(*args, **kwargs):
    def fmt_element(s):
        if s is None:
            return ""
        # Linux => linux
        # SunOS => sunos
        # HP-UX => hpux
        return s.lower().replace("-", "")

    def fmt_modname(args):
        l = ["drivers"]
        for i, e in enumerate(args):
            if e == "":
                continue
            if i == 0:
                if e == "res":
                    e = "resource"
                l.append(e)
            else:
                l.append(fmt_element(e))
        return ".".join(l)

    def import_mod(modname):
        for mn in (modname + "." + fmt_element(rcEnv.sysname), modname):
            try:
                m = importlib.import_module(mn)
                return m
            except ImportError:
                pass

    modname = fmt_modname(args)
    mod = import_mod(modname)

    if mod:
        return mod
    if not kwargs.get("head"):
        kwargs["head"] = modname
    if kwargs.get("fallback", True) and len(args) > 2:
        args = args[:-1]
        return driver_import(*args, **kwargs)
    else:
        raise ImportError("no module found: %s" % kwargs["head"])


def mimport(*args, **kwargs):
    try:
        return driver_import(*args, **kwargs)
    except ImportError:
        pass
    def fmt_element(s):
        if s is None:
            return ""
        if len(s) >= 1:
            return s[0].upper() + s[1:].lower()
        else:
            return ""

    def fmt_modname(args):
        modname = ""
        for i, e in enumerate(args):
            if e in ("res", "prov", "check", "pool") and i == 0:
                modname += e
            else:
                modname += fmt_element(e)
        return modname

    def import_mod(modname):
        for mn in (modname + rcEnv.sysname, modname):
            try:
                return __import__(mn)
            except ImportError:
                pass

    modname = fmt_modname(args)
    mod = import_mod(modname)

    if mod:
        return mod
    if not kwargs.get("head"):
        kwargs["head"] = modname
    if kwargs.get("fallback", True) and len(args) > 1:
        args = args[:-1]
        return mimport(*args, **kwargs)
    else:
        raise ImportError("no module found: %s" % kwargs["head"])


def ximport(base):
    mod = base + rcEnv.sysname
    fpath = os.path.join(rcEnv.paths.pathlib, mod + ".py")
    if not os.path.exists(fpath):
        return __import__(base)
    m = __import__(mod)
    return m


def check_privs():
    if "OSVC_CONTEXT" in os.environ or "OSVC_CLUSTER" in os.environ:
        return
    if os.name == 'nt':
        return
    if os.geteuid() == 0:
        return
    print("Insufficient privileges", file=sys.stderr)
    sys.exit(1)


def banner(text, ch='=', length=78):
    spaced_text = ' %s ' % text
    banner = spaced_text.center(length, ch)
    return banner


def is_exe(fpath, realpath=False):
    """Returns True if file path is executable, False otherwize
    """
    if realpath:
        fpath = os.path.realpath(fpath)
    if os.path.isdir(fpath) or not os.path.exists(fpath):
        return False
    if rcEnv.sysname not in OS_WITHOUT_OS_ACCESS:
        return os.access(fpath, os.X_OK)
    else:
        return os_access_owner_ixusr(fpath)


def os_access_owner_ixusr(path):
    "alternative for os where root user os.access(path, os.X_OK) returns True"
    s_ixusr = 0o0100
    return bool(os.stat(path).st_mode & s_ixusr)


def which(program):
    if program is None:
        return

    def ext_candidates(fpath):
        yield fpath
        for ext in os.environ.get("PATHEXT", "").split(os.pathsep):
            yield fpath + ext

    fpath, fname = os.path.split(program)
    if fpath:
        if os.path.isfile(program) and is_exe(program, realpath=True):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            for candidate in ext_candidates(exe_file):
                if is_exe(candidate, realpath=True):
                    return candidate

    return


def justcall(argv=None, stdin=None, input=None):
    """
    Call subprocess' Popen(argv, stdout=PIPE, stderr=PIPE, stdin=stdin)
    The 'close_fds' value is autodectected (true on unix, false on windows).
    Returns (stdout, stderr, returncode)
    """
    if argv is None:
        argv = [rcEnv.syspaths.false]
    if input:
        stdin = PIPE
        input = bencode(input)
    try:
        process = Popen(argv, stdin=stdin, stdout=PIPE, stderr=PIPE,
                        close_fds=close_fds)
        stdout, stderr = process.communicate(input=input)
        return bdecode(stdout), bdecode(stderr), process.returncode
    except Exception as exc:
        if hasattr(exc, "errno") and getattr(exc, "errno") == 2:
            return "", "", 1
        raise


def empty_string(buff):
    b = buff.strip(' ').strip('\n')
    if len(b) == 0:
        return True
    return False


def lcall(cmd, logger, outlvl=logging.INFO, errlvl=logging.ERROR, timeout=None, **kwargs):
    """
    Variant of subprocess.call that accepts a logger instead of stdout/stderr,
    and logs stdout messages via logger.debug and stderr messages via
    logger.error.
    """
    start = time.time()
    if "close_fds" not in kwargs:
        kwargs["close_fds"] = close_fds
    os.environ["PYTHONIOENCODING"] = "UTF-8"
    rout, wout = os.pipe()
    rerr, werr = os.pipe()
    proc = Popen(cmd, stdout=wout, stderr=werr, **kwargs)
    log_level = {
        rout: outlvl,
        rerr: errlvl
    }
    pending = {
        rout: "",
        rerr: ""
    }
    terminated = False
    killed = False

    def check_io():
        logged = 0
        rlist, _, xlist = select.select([rout, rerr], [], [], 0.2)
        if xlist:
            return logged
        for io in rlist:
            buff = os.read(io, 32768)
            buff = bdecode(buff)
            if six.PY2:
                buff = buff.decode("utf8")
            if buff in ('', b''):
                continue
            buff = pending[io] + buff
            while True:
                l = buff.split("\n", 1)
                if len(l) == 1:
                    pending[io] = l[0]
                    break
                line, buff = l
                if logger:
                    logger.log(log_level[io], "| " + line)
                elif log_level[io] < logging.ERROR:
                    print(line)
                else:
                    print(line, file=sys.stderr)
                logged += 1
        return logged

    # keep checking stdout/stderr until the proc exits
    while proc.poll() is None:
        check_io()
        ellapsed = time.time() - start
        if timeout and ellapsed > timeout:
            if not terminated:
                if logger:
                    logger.error("execution timeout (%.1f seconds). send SIGTERM." % timeout)
                else:
                    print("execution timeout (%.1f seconds). send SIGTERM." % timeout, file=sys.stderr)
                proc.terminate()
                terminated = True
            elif not killed and ellapsed > timeout * 2:
                if logger:
                    logger.error("SIGTERM handling timeout (%.1f seconds). send SIGKILL." % timeout)
                else:
                    print("SIGTERM handling timeout (%.1f seconds). send SIGKILL." % timeout, file=sys.stderr)
                proc.kill()
                killed = True

    while True:
        # check again to catch anything after the process exits
        logged = check_io()
        if logged == 0:
            break
    for io in rout, rerr:
        line = pending[io]
        if line:
            if logger:
                logger.log(log_level[io], "| " + line)
            elif log_level[io] < logging.ERROR:
                print(line)
            else:
                print(line, file=sys.stderr)
    os.close(rout)
    os.close(rerr)
    os.close(wout)
    os.close(werr)
    return proc.returncode


def call(argv,
         cache=False,      # serve/don't serve cmd output from cache
         log=None,         # callers should provide there own logger
                           # or we'll have to allocate a generic one

         info=False,       # False: log cmd as debug
                           # True:  log cmd as info

         outlog=False,     # False: discard stdout

         errlog=True,      # False: discard stderr
                           # True:  log stderr as err, warn or info
                           #        depending on err_to_warn and
                           #        err_to_info value

         outdebug=True,    # True:  log.debug cmd stdout
                           # False: skip log.debug stdout

         errdebug=True,    # True:  log.debug cmd stderr
                           # False: skip log.debug stderr
                           #        depending on err_to_warn and
                           #        err_to_info value
         err_to_warn=False,
         err_to_info=False,
         warn_to_info=False,
         shell=False,
         stdin=None,
         preexec_fn=None,
         cwd=None,
         env=None):
    """
    Execute the command using Popen and return (ret, out, err)
    """
    if log is None:
        log = logging.getLogger('CALL')

    if not argv or len(argv) == 0:
        return (0, '', '')

    if shell:
        cmd = argv
    else:
        cmd = ' '.join(argv)

    if not shell and which(argv[0]) is None:
        log.error("%s does not exist or not in path or is not executable" %
                  argv[0])
        return (1, '', '')

    if info:
        log.info(cmd)
    else:
        log.debug(cmd)

    if not hasattr(rcEnv, "call_cache"):
        rcEnv.call_cache = {}

    if cache and cmd not in rcEnv.call_cache:
        log.debug("cache miss for '%s'" % cmd)

    if not cache or cmd not in rcEnv.call_cache:
        process = Popen(argv, stdin=stdin, stdout=PIPE, stderr=PIPE, close_fds=close_fds, shell=shell,
                        preexec_fn=preexec_fn, cwd=cwd, env=env)
        buff = process.communicate()
        buff = tuple(map(lambda x: bdecode(x).strip(), buff))
        ret = process.returncode
        if ret == 0:
            if cache:
                log.debug("store '%s' output in cache" % cmd)
                rcEnv.call_cache[cmd] = buff
        elif cmd in rcEnv.call_cache:
            log.debug("discard '%s' output from cache because ret!=0" % cmd)
            del rcEnv.call_cache[cmd]
        elif cache:
            log.debug("skip store '%s' output in cache because ret!=0" % cmd)
    else:
        log.debug("serve '%s' output from cache" % cmd)
        buff = rcEnv.call_cache[cmd]
        ret = 0
    if not empty_string(buff[1]):
        if err_to_info:
            log.info('stderr:')
            for line in buff[1].split("\n"):
                log.info("| " + line)
        elif err_to_warn:
            log.warning('stderr:')
            for line in buff[1].split("\n"):
                log.warning("| " + line)
        elif errlog:
            if ret != 0:
                for line in buff[1].split("\n"):
                    log.error("| " + line)
            elif warn_to_info:
                log.info('command successful but stderr:')
                for line in buff[1].split("\n"):
                    log.info("| " + line)
            else:
                log.warning('command successful but stderr:')
                for line in buff[1].split("\n"):
                    log.warning("| " + line)
        elif errdebug:
            log.debug('stderr:')
            for line in buff[1].split("\n"):
                log.debug("| " + line)
    if not empty_string(buff[0]):
        if outlog:
            if ret == 0:
                for line in buff[0].split("\n"):
                    log.info("| " + line)
            elif err_to_info:
                log.info('command failed with stdout:')
                for line in buff[0].split("\n"):
                    log.info("| " + line)
            elif err_to_warn:
                log.warning('command failed with stdout:')
                for line in buff[0].split("\n"):
                    log.warning("| " + line)
            else:
                log.error('command failed with stdout:')
                for line in buff[0].split("\n"):
                    log.error("| " + line)
        elif outdebug:
            log.debug('output:')
            for line in buff[0].split("\n"):
                log.debug("| " + line)

    return (ret, buff[0], buff[1])


def qcall(argv=None):
    """
    Execute command using Popen with no additional args, disgarding stdout and stderr.
    """
    if argv is None:
        argv = [rcEnv.syspaths.false]
    process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=close_fds)
    process.wait()
    return process.returncode


def vcall(args, **kwargs):
    kwargs["info"] = True
    kwargs["outlog"] = True
    return call(args, **kwargs)


def getmount(path):
    path = os.path.abspath(path)
    while path != os.path.sep:
        if not os.path.islink(path) and os.path.ismount(path):
            return path
        path = os.path.abspath(os.path.join(path, os.pardir))
    return path


def protected_dir(path):
    path = path.rstrip("/")
    if path in PROTECTED_DIRS:
        return True
    return False


def protected_mount(path):
    mount = getmount(path)
    if mount in PROTECTED_DIRS:
        return True
    return False


def action_triggers(self, trigger="", action=None, **kwargs):
    """
    Executes a service or resource trigger. Guess if the shell mode is needed
    from the trigger syntax.
    """

    actions = [
        'provision',
        'unprovision',
        'start',
        'stop',
        'shutdown',
        'sync_nodes',
        'sync_drp',
        'sync_all',
        'sync_resync',
        'sync_update',
        'sync_restore',
        'run',
        'command',  # tasks use that as an action
    ]

    compat_triggers = [
        'pre_syncnodes', 'pre_syncdrp',
        'post_syncnodes', 'post_syncdrp',
        'post_syncresync', 'pre_syncresync',
        'post_syncupdate', 'pre_syncupdate',
    ]

    def get_trigger_cmdv(cmd, kwargs):
        """
        Return the cmd arg useable by subprocess Popen
        """
        if not kwargs.get("shell", False):
            if six.PY2:
                cmdv = shlex.split(cmd.encode('utf8'))
                cmdv = [elem.decode('utf8') for elem in cmdv]
            else:
                cmdv = shlex.split(cmd)
        else:
            cmdv = cmd
        return cmdv

    if hasattr(self, "svc"):
        svc = self.svc
        section = self.rid
    else:
        svc = self
        section = "DEFAULT"

    if action not in actions:
        return
    elif action == "startstandby":
        action = "start"
    elif action == "shutdown":
        action = "stop"

    if "blocking" in kwargs:
        blocking = kwargs["blocking"]
        del kwargs["blocking"]
    else:
        blocking = False

    if trigger == "":
        attr = action
    else:
        attr = trigger + "_" + action

    # translate deprecated actions
    if attr in compat_triggers:
        attr = compat_triggers[attr]

    try:
        if attr in self.skip_triggers:
            return
    except AttributeError:
        pass

    try:
        cmd = svc.conf_get(section, attr, use_default=False)
    except ValueError:
        # no corresponding keyword
        return
    except ex.OptNotFound:
        return

    if not cmd:
        svc.log.warning("empty trigger: %s.%s", section, attr)
        return

    if "|" in cmd or "&&" in cmd or ";" in cmd:
        kwargs["shell"] = True

    try:
        cmdv = get_trigger_cmdv(cmd, kwargs)
    except ValueError as exc:
        raise ex.excError(str(exc))

    if not hasattr(self, "log_outputs") or getattr(self, "log_outputs"):
        self.log.info("%s: %s", attr, cmd)

    if svc.options.dry_run:
        return

    try:
        ret = self.lcall(cmdv, **kwargs)
    except OSError as osexc:
        ret = 1
        if osexc.errno == 8:
            self.log.error("%s exec format error: check the script shebang", cmd)
        else:
            self.log.error("%s error: %s", cmd, str(osexc))
    except Exception as exc:
        ret = 1
        self.log.error("%s error: %s", cmd, str(exc))

    if blocking and ret != 0:
        if action == "command":
            raise ex.excError("command return code [%d]" % ret)
        else:
            raise ex.excError("%s: %s blocking error [%d]" % (attr, cmd, ret))

    if not blocking and ret != 0:
        if action == "command":
            self.log.warning("command return code [%d]" % ret)
        else:
            self.log.warning("%s: %s non-blocking error [%d]" % (attr, cmd, ret))


def try_decode(string, codecs=None):
    codecs = codecs or ['utf8', 'latin1']
    for i in codecs:
        try:
            return string.decode(i)
        except:
            pass
    return string


def getaddr_cache_set(name, addr):
    cache_d = os.path.join(rcEnv.paths.pathvar, "cache", "addrinfo")
    makedirs(cache_d)
    cache_f = os.path.join(cache_d, name)
    with open(cache_f, 'w') as f:
        f.write(addr)
    return addr


def getaddr_cache_get(name):
    cache_d = os.path.join(rcEnv.paths.pathvar, "cache", "addrinfo")
    makedirs(cache_d)
    cache_f = os.path.join(cache_d, name)
    if not os.path.exists(cache_f):
        raise Exception("addrinfo cache empty for name %s" % name)
    cache_mtime = datetime.datetime.fromtimestamp(os.stat(cache_f).st_mtime)
    limit_mtime = datetime.datetime.now() - datetime.timedelta(minutes=16)
    if cache_mtime < limit_mtime:
        raise Exception("addrinfo cache expired for name %s (%s)" % (name, cache_mtime.strftime("%Y-%m-%d %H:%M:%S")))
    with open(cache_f, 'r') as f:
        addr = f.read()
    if addr.count(".") != 3 and ":" not in addr:
        raise Exception("addrinfo cache corrupted for name %s: %s" % (name, addr))
    return addr


def getaddr(name, cache_fallback, log=None):
    if cache_fallback:
        return getaddr_caching(name, log=log)
    else:
        return getaddr_non_caching(name)


def getaddr_non_caching(name, log=None):
    a = socket.getaddrinfo(name, None)
    if len(a) == 0:
        raise Exception("could not resolve name %s: empty dns request resultset" % name)
    addr = a[0][4][0]
    try:
        getaddr_cache_set(name, addr)
    except Exception as e:
        if log:
            log.warning("failed to cache name addr %s, %s: %s" % (name, addr, str(e)))
    return addr


def getaddr_caching(name, log=None):
    try:
        addr = getaddr_non_caching(name)
    except Exception as e:
        if log:
            log.warning("%s. fallback to cache." % str(e))
        addr = getaddr_cache_get(name)
    if log:
        log.info("fetched %s address for name %s from cache" % (addr, name))
    return addr


def cidr_to_dotted(s):
    i = int(s)
    _in = ""
    _out = ""
    for i in range(i):
        _in += "1"
    for i in range(32 - i):
        _in += "0"
    _out += str(int(_in[0:8], 2)) + '.'
    _out += str(int(_in[8:16], 2)) + '.'
    _out += str(int(_in[16:24], 2)) + '.'
    _out += str(int(_in[24:32], 2))
    return _out


def to_dotted(s):
    s = str(s)
    if '.' in s:
        return s
    return cidr_to_dotted(s)


def hexmask_to_dotted(mask):
    mask = mask.replace('0x', '')
    s = [str(int(mask[i:i + 2], 16)) for i in range(0, len(mask), 2)]
    return '.'.join(s)


def dotted_to_cidr(mask):
    if mask is None:
        return ''
    cnt = 0
    l = mask.split(".")
    l = map(lambda x: int(x), l)
    for a in l:
        cnt += str(bin(a)).count("1")
    return str(cnt)


def to_cidr(s):
    if s is None:
        return s
    elif '.' in s:
        return dotted_to_cidr(s)
    elif re.match(r"^(0x)*[0-9a-f]{8}$", s):
        # example: 0xffffff00
        s = hexmask_to_dotted(s)
        return dotted_to_cidr(s)
    return s


def term_width():
    min_columns = 78
    detected_columns = _detect_term_width()
    if detected_columns >= min_columns:
        return detected_columns
    else:
        env_columns = int(os.environ.get("COLUMNS", 0))
        if env_columns >= min_columns:
            return env_columns
        else:
            return min_columns


def _detect_term_width():
    try:
        # python 3.3+
        return os.get_terminal_size().columns
    except (AttributeError, OSError):
        pass
    if rcEnv.sysname != "Windows" and which("stty") is not None:
        out, err, ret = justcall(['stty', '-a'])
        if ret == 0:
            m = re.search(r'columns\s+(?P<columns>\d+);', out)
            if m:
                return int(m.group('columns'))
    return 0


def get_cache_d():
    return os.path.join(rcEnv.paths.pathvar, "cache", rcEnv.session_uuid)


def cache(sig):
    def wrapper(fn):
        @wraps(fn)
        def decorator(*args, **kwargs):
            if len(args) > 0 and hasattr(args[0], "log"):
                log = args[0].log
            else:
                log = None

            if len(args) > 0 and hasattr(args[0], "cache_sig_prefix"):
                _sig = args[0].cache_sig_prefix + sig
            else:
                _sig = sig.format(args=args, kwargs=kwargs)

            fpath = cache_fpath(_sig)

            try:
                lfd = lock.lock(timeout=30, delay=0.1, lockfile=fpath + '.lock', intent="cache")
            except Exception as e:
                if log:
                    log.warning("cache locking error: %s. run command uncached." % str(e))
                return fn(*args, **kwargs)
            try:
                data = cache_get(fpath, log=log)
            except Exception as e:
                if log:
                    log.debug(str(e))
                data = fn(*args, **kwargs)
                cache_put(fpath, data, log=log)
            lock.unlock(lfd)
            return data

        return decorator

    return wrapper


def cache_fpath(sig):
    cache_d = get_cache_d()
    makedirs(cache_d)
    sig = sig.replace("/", "(slash)")
    fpath = os.path.join(cache_d, sig)
    return fpath


def cache_put(fpath, data, log=None):
    if log:
        log.debug("cache PUT: %s" % fpath)
    try:
        with open(fpath, "w") as f:
            json.dump(data, f)
    except Exception as e:
        try:
            os.unlink(fpath)
        except:
            pass
    return data


def cache_get(fpath, log=None):
    if not os.path.exists(fpath):
        raise Exception("cache MISS: %s" % fpath)
    if log:
        log.debug("cache GET: %s" % fpath)
    try:
        with open(fpath, "r") as f:
            data = json.load(f)
    except Exception as e:
        raise ex.excError("cache read error: %s" % str(e))
    return data


def clear_cache(sig, o=None):
    if o and hasattr(o, "cache_sig_prefix"):
        sig = o.cache_sig_prefix + sig
    fpath = cache_fpath(sig)
    if not os.path.exists(fpath):
        return
    if o and hasattr(o, "log"):
        o.log.debug("cache CLEAR: %s" % fpath)
    lfd = lock.lock(timeout=30, delay=0.1, lockfile=fpath + '.lock')
    try:
        os.unlink(fpath)
    except:
        pass
    lock.unlock(lfd)


def purge_cache():
    import shutil
    cache_d = get_cache_d()
    try:
        shutil.rmtree(cache_d)
    except:
        pass


def purge_cache_expired():
    import time
    import shutil
    cache_d = os.path.join(rcEnv.paths.pathvar, "cache")
    if not os.path.exists(cache_d) or not os.path.isdir(cache_d):
        return
    for d in os.listdir(cache_d):
        d = os.path.join(cache_d, d)
        if not os.path.isdir(d) or not os.stat(d).st_ctime < time.time() - (21600):
            # session more recent than 6 hours
            continue
        try:
            shutil.rmtree(d)
        except:
            pass


def read_cf(fpaths, defaults=None):
    """
    Read and parse an arbitrary ini-formatted config file, and return
    the RawConfigParser object.
    """
    import codecs
    from rcConfigParser import RawConfigParser
    try:
        from collections import OrderedDict
        config = RawConfigParser(dict_type=OrderedDict)
    except ImportError:
        config = RawConfigParser()

    if defaults is None:
        defaults = {}
    config = RawConfigParser(defaults)
    config.optionxform = str
    if not isinstance(fpaths, (list, tuple)):
        fpaths = [fpaths]
    for fpath in fpaths:
        if not os.path.exists(fpath):
            continue
        with codecs.open(fpath, "r", "utf8") as ofile:
            try:
                if six.PY3:
                    config.read_file(ofile)
                else:
                    config.readfp(ofile)
            except AttributeError:
                raise
    return config


def read_cf_comments(fpath):
    data = {}
    if isinstance(fpath, list):
        return data
    if not os.path.exists(fpath):
        return data
    section = ".header"
    current = []

    import codecs
    with codecs.open(fpath, "r", "utf8") as ofile:
        buff = ofile.read()

    for line in buff.splitlines():
        line = line.strip()
        if not line:
            continue
        if re.match(r"\[.+\]", line):
            if current:
                data[section] = current
                current = []
            section = line[1:-1]
            continue
        if line[0] in (";", "#"):
            stripped = line.lstrip("#;").strip()
            if re.match(r"\[.+\]", stripped):
                # add an empty line before a commented section
                current.append("")
            current.append(stripped)
    if current:
        data[section] = current
        current = []
    return data


def has_option(option, cmd):
    """
    Return True if <option> is set in the <cmd> shlex list.
    """
    for word in cmd:
        if word == option:
            return True
        if word.startswith(option + "="):
            return True
    return False


def get_options(option, cmd):
    """
    Yield all <option> values in the <cmd> shlex list.
    """
    for i, word in enumerate(cmd):
        if word == option:
            yield cmd[i + 1]
        if word.startswith(option + "="):
            yield word.split("=", 1)[-1]


def get_option(option, cmd, boolean=False):
    """
    Get an <option> value in the <cmd> shlex list.
    """
    if boolean and option not in cmd:
        return False
    for i, word in enumerate(cmd):
        if word == option:
            if boolean:
                return True
            else:
                return cmd[i + 1]
        if word.startswith(option + "="):
            return word.split("=", 1)[-1]
    return


def drop_option(option, cmd, drop_value=False):
    """
    Drop an option, and its value if requested, from an argv
    """
    to_drop = []
    for i, word in enumerate(cmd):
        if word == option:
            if drop_value is True:
                to_drop += [i, i + 1]
            elif is_string(drop_value):
                if cmd[i + 1].startswith(drop_value):
                    to_drop += [i, i + 1]
                else:
                    # do not drop option
                    pass
            else:
                to_drop += [i]
            continue
        if word.startswith(option + "="):
            to_drop += [i]
            continue
    for idx in sorted(to_drop, reverse=True):
        del cmd[idx]
    return cmd


def fsum(fpath):
    """
    Return a file content checksum
    """
    import hashlib
    import codecs
    with codecs.open(fpath, "r", "utf-8") as filep:
        buff = filep.read()
    cksum = hashlib.md5(buff.encode("utf-8"))
    return cksum.hexdigest()


def chunker(buff, n):
    """
    Yield successive n-sized chunks from buff
    """
    for i in range(0, len(buff), n):
        yield buff[i:i + n]


def init_locale():
    try:
        locale.setlocale(locale.LC_ALL, ('C', 'UTF-8'))
    except locale.Error:
        pass
    if os.name != "posix":
        return
    locales = ["C.UTF-8", "en_US.UTF-8"]
    for loc in locales:
        if loc not in locale.locale_alias.values():
            continue
        try:
            locale.setlocale(locale.LC_ALL, loc)
        except locale.Error:
            continue
        os.environ["LANG"] = loc
        os.environ["LC_NUMERIC"] = "C"
        os.environ["LC_TIME"] = "C"
        if locale.getlocale()[1] == "UTF-8":
            return
    # raise ex.excError("can not set a C lang with utf8 encoding")
    os.environ["LANG"] = "C"
    os.environ["LC_NUMERIC"] = "C"
    os.environ["LC_TIME"] = "C"


def wipe_rest_markup(payload):
    payload = re.sub(r':(cmd|kw|opt|c-.*?):`(.*?)`', lambda pat: "'" + pat.group(2) + "'", payload, re.MULTILINE)
    payload = re.sub(r'``(.*?)``', lambda pat: "'" + pat.group(1) + "'", payload, re.MULTILINE)
    return payload


#############################################################################
#
# Namespaces functions
#
#############################################################################

def is_service(f, namespace=None, data=None, local=False, kinds=None):
    if f is None:
        return
    f = re.sub(r"\.conf$", '', f)
    f = f.replace(rcEnv.paths.pathetcns + os.sep, "").replace(rcEnv.paths.pathetc + os.sep, "")
    try:
        name, _namespace, kind = split_path(f)
    except ValueError:
        return
    if kinds and kind not in kinds:
        return
    if not namespace:
        namespace = _namespace
    path = fmt_path(name, namespace, kind)
    if not local:
        try:
            data["services"][path]
            return path
        except Exception:
            if want_context():
                return
    cf = svc_pathcf(path)
    if not os.path.exists(cf):
        return
    return path


def list_services(namespace=None, kinds=None):
    l = []
    if namespace in (None, "root"):
        for name in glob_root_config():
            s = name[:-5]
            if len(s) == 0:
                continue
            path = is_service(name, kinds=kinds)
            if path is None:
                continue
            l.append(path)
    n = len(os.path.join(rcEnv.paths.pathetcns, ""))
    for path in glob_ns_config(namespace):
        path = path[n:-5]
        if path[-1] == os.sep:
            continue
        if kinds:
            try:
                name, namespace, kind = split_path(path)
            except ValueError:
                continue
            if kind not in kinds:
                continue
        l.append(path)
    return l


def glob_root_config():
    return chain(
        glob.iglob(GLOB_ROOT_SVC_CONF),
        glob.iglob(GLOB_ROOT_VOL_CONF),
        glob.iglob(GLOB_ROOT_CFG_CONF),
        glob.iglob(GLOB_ROOT_SEC_CONF),
        glob.iglob(GLOB_ROOT_USR_CONF),
    )


def glob_ns_config(namespace=None):
    if namespace is None:
        return glob.iglob(GLOB_CONF_NS)
    else:
        return glob.iglob(GLOB_CONF_NS_ONE % namespace)


def glob_services_config():
    return chain(glob_root_config(), glob_ns_config())


def split_path(path):
    path = path.strip("/")
    if path in ("node", "auth"):
        raise ValueError
    if not path:
        raise ValueError
    if "," in path or "+" in path:
        raise ValueError
    nsep = path.count("/")
    if nsep == 2:
        namespace, kind, name = path.split("/")
    elif nsep == 1:
        kind, name = path.split("/")
        namespace = "root"
    elif nsep == 0:
        name = path
        namespace = "root"
        kind = "svc"
    else:
        raise ValueError(path)
    if namespace == "root":
        namespace = None
        if name == "cluster":
            kind = "ccfg"
    return name, namespace, kind


def svc_pathcf(path, namespace=None):
    name, _namespace, kind = split_path(path)
    if namespace:
        return os.path.join(rcEnv.paths.pathetcns, namespace, kind, name + ".conf")
    elif _namespace:
        return os.path.join(rcEnv.paths.pathetcns, _namespace, kind, name + ".conf")
    elif kind in ("svc", "ccfg"):
        return os.path.join(rcEnv.paths.pathetc, name + ".conf")
    else:
        return os.path.join(rcEnv.paths.pathetc, kind, name + ".conf")


def svc_pathetc(path, namespace=None):
    return os.path.dirname(svc_pathcf(path, namespace=namespace))


def svc_pathtmp(path):
    name, namespace, kind = split_path(path)
    if namespace:
        return os.path.join(rcEnv.paths.pathtmp, "namespaces", namespace, kind)
    elif kind in ("svc", "ccfg"):
        return os.path.join(rcEnv.paths.pathtmp)
    else:
        return os.path.join(rcEnv.paths.pathtmp, kind)


def svc_pathlog(path):
    name, namespace, kind = split_path(path)
    if namespace:
        return os.path.join(rcEnv.paths.pathlog, "namespaces", namespace, kind)
    elif kind in ("svc", "ccfg"):
        return os.path.join(rcEnv.paths.pathlog)
    else:
        return os.path.join(rcEnv.paths.pathlog, kind)


def svc_pathvar(path, relpath=""):
    name, namespace, kind = split_path(path)
    if namespace:
        l = [rcEnv.paths.pathvar, "namespaces", namespace, kind, name]
    else:
        l = [rcEnv.paths.pathvar, kind, name]
    if relpath:
        l.append(relpath)
    return os.path.join(*l)


def fmt_path(name, namespace, kind):
    if namespace:
        return "/".join((namespace.strip("/"), kind, name))
    elif kind not in ("svc", "ccfg"):
        return "/".join((kind, name))
    else:
        return name


def split_fullname(fullname, clustername):
    fullname = fullname[:-(len(clustername) + 1)]
    return fullname.rsplit(".", 2)


def svc_fullname(name, namespace, kind, clustername):
    return "%s.%s.%s.%s" % (
        name,
        namespace if namespace else "root",
        kind,
        clustername
    )


def strip_path(paths, namespace):
    if not namespace:
        return paths
    if isinstance(paths, (list, tuple)):
        return [strip_path(path, namespace) for path in paths]
    else:
        path = re.sub("^%s/" % namespace, "", paths)  # strip current ns
        return re.sub("^svc/", "", path)  # strip default kind


def normalize_path(path):
    name, namespace, kind = split_path(path)
    if namespace is None:
        namespace = "root"
    return fmt_path(name, namespace, kind)


def normalize_paths(paths):
    for path in paths:
        yield normalize_path(path)


def resolve_path(path, namespace=None):
    """
    Return the path, parented in <namespace> if specified and if not found
    in <path>.
    """
    name, _namespace, kind = split_path(path)
    if namespace and not _namespace:
        _namespace = namespace
    if _namespace == "root":
        _namespace = None
    return fmt_path(name, _namespace, kind)


def makedirs(path, mode=0o755):
    """
    Wraps os.makedirs with a more restrictive 755 mode and ignore
    already exists errors.
    """
    try:
        os.makedirs(path, mode)
    except OSError as exc:
        if exc.errno == 17:
            pass
        else:
            raise


def validate_paths(paths):
    [validate_path(p) for p in paths]


def validate_path(path):
    name, namespace, kind = split_path(path)
    validate_kind(kind)
    validate_ns_name(namespace)
    validate_name(name)


def validate_kind(name):
    if name not in rcEnv.kinds:
        raise ValueError("invalid kind '%s'. kind must be one of"
                         " %s." % (name, ", ".join(rcEnv.kinds)))


def validate_ns_name(name):
    if name is None:
        return
    if name in rcEnv.kinds:
        raise ValueError("invalid namespace name '%s'. names must not clash with kinds"
                         " %s." % (name, ", ".join(rcEnv.kinds)))
    if re.match(VALID_NAME_RFC952_NO_DOT, name):
        return
    raise ValueError("invalid namespace name '%s'. names must contain only letters, "
                     "digits and hyphens, start with a letter and end with "
                     "a digit or letter (rfc 952)." % name)


def validate_name(name):
    # strip scaler slice prefix
    name = re.sub(r"^[0-9]+\.", "", name)
    if name in rcEnv.kinds:
        raise ex.excError("invalid name '%s'. names must not clash with kinds"
                          " %s." % (name, ", ".join(rcEnv.kinds)))
    if re.match(VALID_NAME_RFC952, name):
        return
    raise ex.excError("invalid name '%s'. names must contain only dots, letters, "
                      "digits and hyphens, start with a letter and end with "
                      "a digit or letter (rfc 952)." % name)


def factory(kind):
    """
    Return a Svc or Node object
    """
    if kind == "node":
        mod = ximport('node')
        return mod.Node
    if kind == "ccfg":
        from cluster import ClusterSvc
        return ClusterSvc
    try:
        mod = __import__(kind)
        return getattr(mod, kind.capitalize())
    except Exception:
        pass
    raise ValueError("unknown kind: %s" % kind)


def parse_path_selector(selector, namespace=None):
    if selector is None:
        if namespace:
            return "*", namespace, "svc"
        else:
            return "*", "*", "svc"
    elts = selector.split("/")
    elts_count = len(elts)
    if elts_count == 1:
        if elts[0] == "**":
            _namespace = namespace if namespace else "*"
            _kind = "*"
            _name = "*"
        elif elts[0] == "*":
            _namespace = namespace if namespace else "*"
            _kind = "svc"
            _name = "*"
        else:
            _namespace = namespace if namespace else "*"
            _kind = "svc"
            _name = elts[0]
    elif elts_count == 2:
        if elts[0] == "**":
            _namespace = namespace if namespace else "*"
            _kind = "*"
            _name = elts[1] if elts[1] not in ("**", "") else "*"
        elif elts[0] == "*":
            _namespace = namespace if namespace else "*"
            _kind = "svc"
            _name = elts[1] if elts[1] not in ("**", "") else "*"
        elif elts[1] == "**":
            _namespace = namespace if namespace else elts[0]
            _kind = "*"
            _name = "*"
        elif elts[0] == "*":
            _namespace = namespace if namespace else elts[0]
            _kind = "svc"
            _name = "*"
        else:
            _namespace = "root"
            _kind = elts[0]
            _name = elts[1]
    elif elts_count == 3:
        _namespace = namespace if namespace else elts[0]
        _kind = elts[1]
        _name = elts[2]
    else:
        raise ValueError("invalid path selector %s" % selector)
    return _name, _namespace, _kind


def format_path_selector(selector, namespace=None):
    try:
        _name, _namespace, _kind = parse_path_selector(selector, namespace)
    except ValueError:
        return selector
    return "%s/%s/%s" % (_namespace, _kind, _name)


def normalize_jsonpath(path):
    if path and path[0] == ".":
        path = path[1:]
    return path


def abbrev(l):
    if len(l) < 1:
        return l
    paths = [n.split(".")[::-1] for n in l]
    trimable = [n for n in paths if len(n) > 1]
    if len(trimable) <= 1:
        return [n[-1] + ".." if n in trimable else n[0] for n in paths]
    for i in range(10):
        try:
            if len(set([t[i] for t in trimable])) > 1:
                break
        except IndexError:
            break
    if i == 0:
        return l
    return [".".join(n[:i - 1:-1]) + ".." if n in trimable else n[0] for n in paths]


def process_args(pid):
    cmd_args = ['/bin/ps', '-p', str(pid), '-o', 'args=']
    ret, stdout, stderr = call(cmd_args)
    if ret != 0:
        return False, ''
    else:
        return True, stdout


def process_match_args(pid, search_args=None):
    running, args = process_args(pid)
    if running:
        return search_args in args
    else:
        return False


def daemon_process_running():
    try:
        with open(rcEnv.paths.daemon_pid, 'r') as pid_file:
            pid = int(pid_file.read())
        with open(rcEnv.paths.daemon_pid_args, 'r') as pid_args_file:
            search_args = pid_args_file.read()
        return process_match_args(pid, search_args=search_args)
    except:
        return False


def is_glob(text):
    if len(set(text) & set("?*[")) > 0:
        return True
    return False


def find_editor():
    if "EDITOR" in os.environ:
        editor = os.environ["EDITOR"]
    elif os.name == "nt":
        editor = "notepad"
    else:
        editor = "vi"
    if not which(editor):
        raise ex.excError("%s not found" % editor)
    return editor


def create_protected_file(filepath, buff, mode):
    with open(filepath, mode) as f:
        if os.name == 'posix':
            os.chmod(filepath, 0o0600)
        f.write(buff)


def iter_drivers(groups=None):
    import importlib
    import pkgutil
    groups = groups or [""]
    for group in groups:
        try:
            package = importlib.import_module("drivers.resource."+group)
        except ImportError:
            continue
        for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
            if not ispkg:
                continue
            yield mimport("resource", group, modname)

