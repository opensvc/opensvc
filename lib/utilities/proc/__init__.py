import logging
import os
import sys
import select
import time
from errno import ENOENT
from subprocess import Popen, PIPE

import six
from rcGlobalEnv import rcEnv
from utilities.string import bencode, bdecode, empty_string

# Os where os.access is invalid
OS_WITHOUT_OS_ACCESS = ['SunOS']

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
        argv = [rcEnv.syspaths.false]
    if input:
        stdin = PIPE
        input = bencode(input)
    try:
        proc = Popen(argv, stdin=stdin, stdout=PIPE, stderr=PIPE,
                     close_fds=close_fds)
        out, err = proc.communicate(input=input)
        return bdecode(out), bdecode(err), proc.returncode
    except Exception as exc:
        if hasattr(exc, "errno") and getattr(exc, "errno") == ENOENT:
            return "", "", 1
        raise


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



