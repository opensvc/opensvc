#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import os, sys
import logging
from subprocess import *
from rcGlobalEnv import rcEnv

def ximport(base):
    mod = base + rcEnv.sysname
    try:
        m = __import__(mod)
        return m
    except:
        pass

    return __import__(base)

def fork(fn, kwargs):
    if os.fork() > 0:
        """ return to parent execution
        """
        return pid

    """ separate the son from the father
    """
    os.chdir('/')
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)
    except:
        os._exit(1)

    fn(**kwargs)
    os._exit(0)

def check_privs():
    if os.name == 'nt':
        return
    if os.getuid() != 0:
        print('Insufficient privileges. Try:\n sudo ' + ' '.join(sys.argv))
        sys.exit(1)


def banner(text, ch='=', length=78):
    spaced_text = ' %s ' % text
    banner = spaced_text.center(length, ch)
    return banner

def is_exe(fpath):
    """Returns True if file path is executable, False otherwize
    does not follow symlink
    """
    return os.path.exists(fpath) and os.access(fpath, os.X_OK)

def which(program):
    def ext_candidates(fpath):
        yield fpath
        for ext in os.environ.get("PATHEXT", "").split(os.pathsep):
            yield fpath + ext

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            for candidate in ext_candidates(exe_file):
                if is_exe(candidate):
                    return candidate

    return None

def justcall(argv=['/bin/false']):
    """subprosses call argv, return (stdout,stderr,returncode)
    """
    if which(argv[0]) is None:
        return ("", "", 1)
    if os.name == 'nt':
        close_fds = False
    else:
        close_fds = True
    process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=close_fds)
    stdout, stderr = process.communicate(input=None)
    if sys.version_info[0] < 3:
        return stdout, stderr, process.returncode
    else:
        return str(stdout, "ascii"), str(stderr, "ascii"), process.returncode

def empty_string(buff):
    b = buff.strip(' ').strip('\n')
    if len(b) == 0:
        return True
    return False

def call(argv=['/bin/false'],
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
         err_to_info=False):
    "return(ret, stdout,stderr)"
    if log is None:
        log = logging.getLogger('CALL')
    if not argv or len(argv) == 0:
        return (0, '', '')
    if which(argv[0]) is None:
        log.error("%s does not exist or not in path or is not executable"%
                  argv[0])
        return (1, '', '')
    cmd = ' '.join(argv)
    if info:
        log.info(cmd)
    else:
        log.debug(cmd)
    if not hasattr(rcEnv, "call_cache"):
        rcEnv.call_cache = {}
    if not cache or cmd not in rcEnv.call_cache:
        if not cache:
            log.debug("caching for '%s' explicitely disabled"%cmd)
        elif cmd not in rcEnv.call_cache:
            log.debug("cache miss for '%s'"%cmd)
        process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if sys.version_info[0] >= 3:
            buff = tuple(map(lambda x: str(x, "ascii"), buff))
        ret = process.returncode
        if ret == 0:
            log.debug("store '%s' output in cache"%cmd)
            rcEnv.call_cache[cmd] = buff
        elif cmd in rcEnv.call_cache:
            log.debug("discard '%s' output from cache because ret!=0"%cmd)
            del rcEnv.call_cache[cmd]
        else:
            log.debug("skip store '%s' output in cache because ret!=0"%cmd)
    else:
        log.debug("serve '%s' output from cache"%cmd)
        buff = rcEnv.call_cache[cmd]
        ret = 0
    if not empty_string(buff[1]):
        if err_to_info:
            log.info('stderr:\n' + buff[1])
        elif err_to_warn:
            log.warning('stderr:\n' + buff[1])
        elif errlog:
            if ret != 0:
                log.error('stderr:\n' + buff[1])
            else:
                log.warning('command succesful but stderr:\n' + buff[1])
        elif errdebug:
            log.debug('stderr:\n' + buff[1])
    if not empty_string(buff[0]):
        if outlog:
            if ret == 0:
                log.info('output:\n' + buff[0])
            elif err_to_info:
                log.info('command failed with stdout:\n' + buff[0])
            elif err_to_warn:
                log.warning('command failed with stdout:\n' + buff[0])
            else:
                log.error('command failed with stdout:\n' + buff[0])
        elif outdebug:
            log.debug('output:\n' + buff[0])

    return (ret, buff[0], buff[1])

def qcall(argv=['/bin/false']) :
    """qcall Launch Popen it args disgarding output and stderr"""
    if not argv:
        return (0, '')
    process = Popen(argv, stdout=open('/dev/null'), stderr=open('/dev/null'), close_fds=True)
    process.wait()
    return process.returncode

def vcall(argv=['/bin/false'],
          log=None,
          err_to_warn=False,
          err_to_info=False ):
    return call(argv,
                log=log,
                info=True,
                outlog=True,
                err_to_warn=err_to_warn,
                err_to_info=err_to_info)

def getmount(path):
    path = os.path.abspath(path)
    while path != os.path.sep:
        if not os.path.islink(path) and os.path.ismount(path):
            return path
        path = os.path.abspath(os.path.join(path, os.pardir))
    return path

def protected_mount(path):
    if getmount(path) in ['/', '/usr', '/var', '/sys', '/proc', '/tmp', '/opt', '/dev', '/dev/pts', '/home', '/boot', '/dev/shm']:
        return True
    return False


if __name__ == "__main__":
    print("call(('id','-a'))")
    (r,output,err)=call(("/usr/bin/id","-a"))
    print("status: ", r, "output:", output)

