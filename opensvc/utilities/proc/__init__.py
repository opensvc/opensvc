from __future__ import print_function

import locale
import logging
import os
import select
import shlex
import sys
import time
from errno import ENOENT, EACCES
from subprocess import Popen, PIPE

import core.exceptions as ex
import foreign.six as six
from env import Env
from utilities.string import bencode, bdecode, empty_string, is_string

# Os where os.access is invalid
OS_WITHOUT_OS_ACCESS = ['SunOS']

# lcall checkio() default timeout
LCALL_CHECK_IO_TIMEOUT = 0.2

if os.name == 'nt':
    close_fds = False
else:
    close_fds = True


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
        argv = [Env.syspaths.false]
    if input:
        stdin = PIPE
        input = bencode(input)
    try:
        proc = Popen(argv, stdin=stdin, stdout=PIPE, stderr=PIPE,
                     close_fds=close_fds)
        out, err = proc.communicate(input=input)
        return bdecode(out), bdecode(err), proc.returncode
    except OSError as exc:
        if exc.errno in (ENOENT, EACCES):
            return "", "", 1
        raise


def is_exe(fpath, realpath=False):
    """Returns True if file path is executable, False otherwize
    """
    if realpath:
        fpath = os.path.realpath(fpath)
    if os.path.isdir(fpath) or not os.path.exists(fpath):
        return False
    if Env.sysname not in OS_WITHOUT_OS_ACCESS:
        return os.access(fpath, os.X_OK)
    else:
        return os_access_owner_ixusr(fpath)


def os_access_owner_ixusr(path):
    "alternative for os where root user os.access(path, os.X_OK) returns True"
    s_ixusr = 0o0100
    return bool(os.stat(path).st_mode & s_ixusr)


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
        rlist, _, xlist = select.select([rout, rerr], [], [], LCALL_CHECK_IO_TIMEOUT)
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

    if info:
        log.info(cmd)
    else:
        log.debug(cmd)

    if not hasattr(Env, "call_cache"):
        Env.call_cache = {}

    if cache and cmd not in Env.call_cache:
        log.debug("cache miss for '%s'" % cmd)

    if not cache or cmd not in Env.call_cache:
        try:
            process = Popen(argv, stdin=stdin, stdout=PIPE, stderr=PIPE, close_fds=close_fds, shell=shell,
                            preexec_fn=preexec_fn, cwd=cwd, env=env)
        except OSError as exc:
            if exc.errno == EACCES:
                log.error("command is not executable")
                return 1, "", ""
            elif exc.errno == ENOENT:
                log.error("command not found")
                return 1, "", ""
            raise
        buff = process.communicate()
        buff = tuple(map(lambda x: bdecode(x).strip(), buff))
        ret = process.returncode
        if ret == 0:
            if cache:
                log.debug("store '%s' output in cache" % cmd)
                Env.call_cache[cmd] = buff
        elif cmd in Env.call_cache:
            log.debug("discard '%s' output from cache because ret!=0" % cmd)
            del Env.call_cache[cmd]
        elif cache:
            log.debug("skip store '%s' output in cache because ret!=0" % cmd)
    else:
        log.debug("serve '%s' output from cache" % cmd)
        buff = Env.call_cache[cmd]
        ret = 0
    if not empty_string(buff[1]):
        if err_to_info:
            log.info('stderr:')
            call_log(buff[1], log, "info")
        elif err_to_warn:
            log.warning('stderr:')
            call_log(buff[1], log, "warning")
        elif errlog:
            if ret != 0:
                call_log(buff[1], log, "error")
            elif warn_to_info:
                log.info('command successful but stderr:')
                call_log(buff[1], log, "info")
            else:
                log.warning('command successful but stderr:')
                call_log(buff[1], log, "warning")
        elif errdebug:
            log.debug('stderr:')
            call_log(buff[1], log, "debug")
    if not empty_string(buff[0]):
        if outlog:
            if ret == 0:
                call_log(buff[0], log, "info")
            elif err_to_info:
                log.info('command failed with stdout:')
                call_log(buff[0], log, "info")
            elif err_to_warn:
                log.warning('command failed with stdout:')
                call_log(buff[0], log, "warning")
            else:
                log.error('command failed with stdout:')
                call_log(buff[0], log, "error")
        elif outdebug:
            log.debug('output:')
            call_log(buff[0], log, "debug")

    return (ret, buff[0], buff[1])


def qcall(argv=None):
    """
    Execute command using Popen with no additional args, disgarding stdout and stderr.
    """
    if argv is None:
        return 1
    process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=close_fds)
    process.wait()
    return process.returncode


def vcall(args, **kwargs):
    kwargs["info"] = True
    kwargs["outlog"] = True
    return call(args, **kwargs)


def check_privs():
    if "OSVC_CONTEXT" in os.environ or "OSVC_CLUSTER" in os.environ:
        return
    if os.name == 'nt':
        return
    if os.geteuid() == 0:
        return
    print("Insufficient privileges", file=sys.stderr)
    sys.exit(1)


def action_triggers(self, trigger="", action=None, shell=False, **kwargs):
    """
    Executes a service or resource trigger. Guess if the shell mode is needed
    from the trigger syntax.
    """

    actions = [
        'provision',
        'unprovision',
        'start',
        'startstandby',
        'stop',
        'shutdown',
        'sync_nodes',
        'sync_drp',
        'sync_all',
        'sync_resync',
        'sync_update',
        'sync_restore',
        'run',
        'on_error', # tasks use that as an action
        'command',  # tasks use that as an action
    ]

    if hasattr(self, "svc"):
        svc = self.svc
        section = self.rid
    else:
        svc = self
        section = "DEFAULT"

    if action not in actions:
        return
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

    try:
        if does_call_cmd_need_shell(cmd):
            shell = True
        cmdv = get_call_cmd_from_str(cmd, shell=shell)
    except ValueError as exc:
        raise ex.Error(str(exc))

    if not hasattr(self, "log_outputs") or getattr(self, "log_outputs"):
        self.log.info("%s: %s", attr, cmd)

    if svc.options.dry_run:
        return

    try:
        ret = self.lcall(cmdv, shell=shell, **kwargs)
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
            raise ex.Error("command return code [%d]" % ret)
        else:
            raise ex.Error("%s: %s blocking error [%d]" % (attr, cmd, ret))

    if not blocking and ret != 0:
        if action == "command":
            self.log.warning("command return code [%d]" % ret)
        else:
            self.log.warning("%s: %s non-blocking error [%d]" % (attr, cmd, ret))


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
    # raise ex.Error("can not set a C lang with utf8 encoding")
    os.environ["LANG"] = "C"
    os.environ["LC_NUMERIC"] = "C"
    os.environ["LC_TIME"] = "C"


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
        with open(Env.paths.daemon_pid, 'r') as pid_file:
            pid = int(pid_file.read())
        with open(Env.paths.daemon_pid_args, 'r') as pid_args_file:
            search_args = pid_args_file.read()
        return process_match_args(pid, search_args=search_args)
    except:
        return False


def find_editor():
    if "EDITOR" in os.environ:
        editor = os.environ["EDITOR"]
    elif os.name == "nt":
        editor = "notepad"
    else:
        editor = "vi"
    if not which(editor):
        raise ex.Error("%s not found" % editor)
    return editor


def get_extra_argv(argv=None):
    """
    Extract extra argv from "om array" and "om collector cli" argv.

    om node act as a wrapper for other commands (storage drivers for
    example).
    """
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) < 2:
        return argv, []

    if "array" in argv:
        if argv in (["array", "ls"], ["array", "show"]):
            return argv, []
        pos = argv.index('array')
    elif "cli" in argv:
        pos = argv.index('cli')
        if pos > 0 and argv[pos-1] != "collector":
            return argv, []
    else:
        return argv, []

    if "--" in argv:
        pos = argv.index("--")
    if len(argv) > pos + 1:
        extra_argv = argv[pos+1:]
    else:
        extra_argv = []
    argv = argv[:pos+1]
    return argv, extra_argv


def call_log(buff="", log=None, level="info"):
    if not buff:
        return
    lines = buff.rstrip().split("\n")
    try:
        fn = getattr(log, level)
    except Exception:
        return
    for line in lines:
        fn("| " + line)


def get_call_cmd_from_str(cmd, shell=False):
    """
    Return the cmd arg usable by ?call
    """
    if shell:
        return cmd
    else:
        if six.PY2:
            cmdv = shlex.split(cmd.encode('utf8'))
            return [elem.decode('utf8') for elem in cmdv]
        else:
            return shlex.split(cmd)

def does_call_cmd_need_shell(cmd):
    return "|" in cmd or "&&" in cmd or ";" in cmd
