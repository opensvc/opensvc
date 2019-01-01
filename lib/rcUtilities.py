from __future__ import print_function

import ast
import datetime
import json
import glob
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

import six
import lock
import rcExceptions as ex
from rcGlobalEnv import rcEnv

GLOB_SVC_CONF = os.path.join(rcEnv.paths.pathetc, "*.conf")
GLOB_SVC_CONF_NS = os.path.join(rcEnv.paths.pathetcns, "*", "*.conf")

ANSI_ESCAPE = re.compile(r"\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[m|H|J|K|G]", re.UNICODE)
ANSI_ESCAPE_B = re.compile(b"\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[m|H|J|K|G]")

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
        if isinstance(node, ast.Num): # <number>
            return node.n
        elif isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Name):
            if node.id in _safe_names:
                return _safe_names[node.id]
            return node.id
        elif isinstance(node, ast.Tuple):
            return tuple(node.elts)
        elif isinstance(node, ast.BinOp): # <left> <operator> <right>
            return operators[type(node.op)](eval_(node.left), eval_(node.right))
        elif isinstance(node, ast.UnaryOp): # <operator> <operand> e.g., -1
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

def mimport(*args, **kwargs):
    def fmt(s):
        if len(s) >= 1:
            return s[0].upper()+s[1:].lower()
        else:
            return ""

    mod = ""
    for i, e in enumerate(args):
        if e in ("res", "prov", "check", "pool") and i == 0:
            mod += e
        else:
            mod += fmt(e)

    try:
        return __import__(mod+rcEnv.sysname)
    except ImportError:
        pass

    try:
        return __import__(mod)
    except ImportError as exc:
        pass

    if kwargs.get("fallback", True) and len(args) > 1:
        args = args[:-1]
        return mimport(*args, **kwargs)
    else:
        raise ImportError("no module found")

def ximport(base):
    mod = base + rcEnv.sysname
    fpath = os.path.join(rcEnv.paths.pathlib, mod+".py")
    if not os.path.exists(fpath):
        return __import__(base)
    m = __import__(mod)
    return m

def check_svclink_ns(svclink, namespace):
    if namespace and "/namespaces/%s/" % namespace not in svclink:
        raise ex.excError("Service link '%s' doesn't belong to namespace '%s'.\n"
                          "Use a service selector expression to select a "
                          "service from a foreign namespace." % (os.path.basename(svclink), namespace))

def check_privs():
    if os.name == 'nt':
        return
    if os.geteuid() == 0:
        return
    import copy
    l = copy.copy(sys.argv)
    env = rcEnv.initial_env
    namespace = env.get("OSVC_NAMESPACE")
    svclink = env.get("OSVC_SERVICE_LINK")
    if svclink:
        path = svcpath_from_link(svclink)
        try:
            check_svclink_ns(svclink, namespace)
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            sys.exit(1)
        l[0] = os.path.join(rcEnv.paths.pathbin, "svcmgr")
        l.insert(1, "--service=%s" % path)
    else:
        l[0] = os.path.join(rcEnv.paths.pathbin, os.path.basename(l[0]).replace(".py", ""))
    if namespace and "--namespace" not in l and ("svcmgr" in l[0] or "svcmon" in l[0]):
        l.insert(1, "--namespace=%s" % namespace)
    if which("sudo"):
        os.execvpe("sudo", ["sudo"] + l, env=env)
    elif which("pfexec"):
        os.execvpe("sudo", ["pfexec"] + l, env=env)
    else:
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
    if os.path.isdir(fpath):
        return False
    return os.path.exists(fpath) and os.access(fpath, os.X_OK)

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

def justcall(argv=['/bin/false'], stdin=None):
    """
    Call subprocess' Popen(argv, stdout=PIPE, stderr=PIPE, stdin=stdin)
    The 'close_fds' value is autodectected (true on unix, false on windows).
    Returns (stdout, stderr, returncode)
    """
    try:
        process = Popen(argv, stdin=stdin, stdout=PIPE, stderr=PIPE,
                        close_fds=close_fds)
        stdout, stderr = process.communicate(input=None)
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
                logger.log(log_level[io], line)
                logged += 1
        return logged

    # keep checking stdout/stderr until the proc exits
    while proc.poll() is None:
        check_io()
        ellapsed = time.time() - start
        if timeout and ellapsed > timeout:
            if not terminated:
                logger.error("execution timeout (%.1f seconds). send SIGTERM." % timeout)
                proc.terminate()
                terminated = True
            elif not killed and ellapsed > timeout*2:
                logger.error("SIGTERM handling timeout (%.1f seconds). send SIGKILL." % timeout)
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
            logger.log(log_level[io], line)
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
        log.error("%s does not exist or not in path or is not executable"%
                  argv[0])
        return (1, '', '')

    if info:
        log.info(cmd)
    else:
        log.debug(cmd)

    if not hasattr(rcEnv, "call_cache"):
        rcEnv.call_cache = {}

    if cache and cmd not in rcEnv.call_cache:
        log.debug("cache miss for '%s'"%cmd)

    if not cache or cmd not in rcEnv.call_cache:
        process = Popen(argv, stdin=stdin, stdout=PIPE, stderr=PIPE, close_fds=close_fds, shell=shell, preexec_fn=preexec_fn, cwd=cwd, env=env)
        buff = process.communicate()
        buff = tuple(map(lambda x: bdecode(x).strip(), buff))
        ret = process.returncode
        if ret == 0:
            if cache:
                log.debug("store '%s' output in cache"%cmd)
                rcEnv.call_cache[cmd] = buff
        elif cmd in rcEnv.call_cache:
            log.debug("discard '%s' output from cache because ret!=0"%cmd)
            del rcEnv.call_cache[cmd]
        elif cache:
            log.debug("skip store '%s' output in cache because ret!=0"%cmd)
    else:
        log.debug("serve '%s' output from cache"%cmd)
        buff = rcEnv.call_cache[cmd]
        ret = 0
    if not empty_string(buff[1]):
        if err_to_info:
            log.info('stderr:')
            for line in buff[1].split("\n"):
                log.info(line)
        elif err_to_warn:
            log.warning('stderr:')
            for line in buff[1].split("\n"):
                log.warning(line)
        elif errlog:
            if ret != 0:
                log.error('stderr:')
                for line in buff[1].split("\n"):
                    log.error(line)
            elif warn_to_info:
                log.info('command successful but stderr:')
                for line in buff[1].split("\n"):
                    log.info(line)
            else:
                log.warning('command successful but stderr:')
                for line in buff[1].split("\n"):
                    log.warning(line)
        elif errdebug:
            log.debug('stderr:')
            for line in buff[1].split("\n"):
                log.debug(line)
    if not empty_string(buff[0]):
        if outlog:
            if ret == 0:
                log.info('output:')
                for line in buff[0].split("\n"):
                    log.info(line)
            elif err_to_info:
                log.info('command failed with stdout:')
                for line in buff[0].split("\n"):
                    log.info(line)
            elif err_to_warn:
                log.warning('command failed with stdout:')
                for line in buff[0].split("\n"):
                    log.warning(line)
            else:
                log.error('command failed with stdout:')
                for line in buff[0].split("\n"):
                    log.error(line)
        elif outdebug:
            log.debug('output:')
            for line in buff[0].split("\n"):
                log.debug(line)

    return (ret, buff[0], buff[1])

def qcall(argv=['/bin/false']):
    """
    Execute command using Popen with no additional args, disgarding stdout and stderr.
    """
    if not argv:
        return 0
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
        'command', # tasks use that as an action
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
        attr = trigger+"_"+action

    # translate deprecated actions
    if attr in compat_triggers:
        attr = compat_triggers[attr]

    try:
        cmd = svc.conf_get(section, attr, use_default=False)
    except ValueError:
        # no corresponding keyword
        return
    except ex.OptNotFound:
        return

    if cmd is None:
        svc.log.warning("empty trigger: %s.%s", section, attr)
        return

    if "|" in cmd or "&&" in cmd or ";" in cmd:
        kwargs["shell"] = True

    cmdv = get_trigger_cmdv(cmd, kwargs)

    self.log.info("%s: %s", attr, cmd)

    if svc.options.dry_run:
        return

    try:
        ret = self.lcall(cmdv, **kwargs)
    except OSError as exc:
        ret = 1
        if exc.errno == 8:
            self.log.error("%s exec format error: check the script shebang", cmd)
        else:
            self.log.error("%s error: %s", cmd, str(exc))
    except Exception as exc:
        ret = 1
        self.log.error("%s error: %s", cmd, str(exc))

    if blocking and ret != 0:
        raise ex.excError("%s trigger %s blocking error" % (trigger, cmd))


def try_decode(string, codecs=['utf8', 'latin1']):
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
            log.warning("failed to cache name addr %s, %s: %s"  %(name, addr, str(e)))
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
    for i in range(32-i):
        _in += "0"
    _out += str(int(_in[0:8], 2))+'.'
    _out += str(int(_in[8:16], 2))+'.'
    _out += str(int(_in[16:24], 2))+'.'
    _out += str(int(_in[24:32], 2))
    return _out

def to_dotted(s):
    s = str(s)
    if '.' in s:
        return s
    return cidr_to_dotted(s)

def hexmask_to_dotted(mask):
    mask = mask.replace('0x', '')
    s = [str(int(mask[i:i+2], 16)) for i in range(0, len(mask), 2)]
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
    default = int(os.environ.get("COLUMNS", 78))
    try:
        # python 3.3+
        return os.get_terminal_size().columns
    except (AttributeError, OSError):
        pass
    if rcEnv.sysname == "Windows":
        return default
    if which("stty") is None:
        return default
    out, err, ret = justcall(['stty', '-a'])
    m = re.search('columns\s+(?P<columns>\d+);', out)
    if m:
        return int(m.group('columns'))
    return default

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
                lfd = lock.lock(timeout=30, delay=0.1, lockfile=fpath+'.lock', intent="cache")
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
    lfd = lock.lock(timeout=30, delay=0.1, lockfile=fpath+'.lock')
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
        if not os.path.isdir(d) or not os.stat(d).st_ctime < time.time()-(21600):
            # session more recent than 6 hours
            continue
        try:
            shutil.rmtree(d)
        except:
            pass

def read_cf(fpath, defaults=None):
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
    if not os.path.exists(fpath):
        return config
    with codecs.open(fpath, "r", "utf8") as ofile:
        try:
            if six.PY3:
                config.read_file(ofile)
            else:
                config.readfp(ofile)
        except AttributeError:
            raise
    return config

def has_option(option, cmd):
    """
    Return True if <option> is set in the <cmd> shlex list.
    """
    for word in cmd:
        if word == option:
            return True
        if word.startswith(option+"="):
            return True
    return False

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
                return cmd[i+1]
        if word.startswith(option+"="):
            return word.split("=", 1)[-1]
    return

def drop_option(option, cmd, drop_value=False):
    """
    Drop an option, and its value if requested, from an argv
    """
    to_drop = []
    for i, word in enumerate(cmd):
        if word == option:
            if drop_value:
                to_drop += [i, i+1]
            else:
                to_drop += [i]
            continue
        if word.startswith(option+"="):
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
        yield buff[i:i+n]

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
        if locale.getlocale()[1] == "UTF-8":
            return
    #raise ex.excError("can not set a C lang with utf8 encoding")
    os.environ["LANG"] = "C"
    os.environ["LC_NUMERIC"] = "C"

#############################################################################
#
# Namespaces functions
#
#############################################################################

def is_service(f, namespace=None):
    if f is None:
        return
    if f.startswith("root/"):
        f = f[5:]
    if f.count("/") > 1:
        return
    basename = os.path.basename(f)
    if basename in ["node", "auth"]:
        return
    if basename == f and not namespace:
        f = os.path.join(rcEnv.paths.pathetc, f)
    elif f.startswith(os.sep):
        pass
    elif namespace:
        f = os.path.join(rcEnv.paths.pathetcns, namespace, f)
    else:
        f = os.path.join(rcEnv.paths.pathetcns, f)
    if os.name != "nt" and os.path.realpath(f) != os.path.realpath(rcEnv.paths.svcmgr):
        return
    if not os.path.exists(f + '.conf'):
        return
    return f.replace(rcEnv.paths.pathetcns+os.sep, "").replace(rcEnv.paths.pathetc+os.sep, "")

def list_services(namespace=None):
    makedirs(rcEnv.paths.pathetc)
    l = []
    if namespace is None:
        s = glob.glob(GLOB_SVC_CONF)
        s = [x[:-5] for x in s]
        for name in s:
            if len(s) == 0:
                continue
            svcpath = is_service(name)
            if svcpath is None:
                continue
            l.append(svcpath)
        s = glob.glob(GLOB_SVC_CONF_NS)
    else:
        s = glob.glob(os.path.join(rcEnv.paths.pathetcns, namespace, "*.conf"))
    n = len(os.path.join(rcEnv.paths.pathetcns, ""))
    for path in s:
        path = path[n:-5]
        if path[-1] == os.sep:
            continue
        l.append(path)
    return l

def glob_services_config():
    return glob.glob(GLOB_SVC_CONF) + glob.glob(GLOB_SVC_CONF_NS)

def svcpath_from_link(svclink):
    try:
        l = svclink.split(os.sep)
        l = l[l.index("namespaces")+1:]
        return os.path.join(*l)
    except Exception as exc:
        return os.path.basename(svclink)

def split_svcpath(path):
    path = path.strip("/")
    if not path:
        raise ValueError
    svcname = os.path.basename(path)
    namespace = os.path.dirname(path)
    if namespace == "root":
        namespace = None
    return svcname, namespace

def svc_pathcf(path, namespace=None):
    name, _namespace = split_svcpath(path)
    if namespace:
        return os.path.join(rcEnv.paths.pathetcns, namespace, name+".conf")
    elif _namespace:
        return os.path.join(rcEnv.paths.pathetcns, _namespace, name+".conf")
    else:
        return os.path.join(rcEnv.paths.pathetc, name+".conf")

def svc_pathetc(path, namespace=None):
    return os.path.dirname(svc_pathcf(path, namespace=namespace))

def svc_pathvar(path, relpath=""):
    name, namespace = split_svcpath(path)
    if namespace:
        l = [rcEnv.paths.pathvar, "namespaces", namespace, "services", name]
    else:
        l = [rcEnv.paths.pathvar, "services", name]
    if relpath:
        l.append(relpath)
    return os.path.join(*l)

def fmt_svcpath(name, namespace):
    if namespace:
        return "/".join((namespace.strip("/"), name))
    else:
        return name

def exe_link_exists(svcname, namespace):
    if os.name != 'posix':
        return False
    pathetc = svc_pathetc(svcname, namespace)
    try:
        p = os.readlink(os.path.join(pathetc, svcname))
        if p == rcEnv.paths.svcmgr:
            return True
        else:
            return False
    except:
        return False

def fix_exe_link(svcname, namespace):
    """
    Create the <svcname> -> svcmgr symlink
    """
    if os.name != 'posix':
        return
    pathetc = svc_pathetc(svcname, namespace)
    if not os.path.exists(os.path.join(pathetc, svcname+".conf")):
        return
    if not exe_link_exists(svcname, namespace):
        from freezer import Freezer
        svcpath = fmt_svcpath(svcname, namespace)
        Freezer(svcpath).freeze()
    os.chdir(pathetc)
    try:
        p = os.readlink(svcname)
    except:
        os.symlink(rcEnv.paths.svcmgr, svcname)
        p = rcEnv.paths.svcmgr
    if p != rcEnv.paths.svcmgr:
        os.unlink(svcname)
        os.symlink(rcEnv.paths.svcmgr, svcname)

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
