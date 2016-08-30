from __future__ import print_function
import os, sys
import datetime
import logging
import socket
import re
from subprocess import *
from rcGlobalEnv import rcEnv

protected_dirs = ['/', '/usr', '/var', '/sys', '/proc', '/tmp', '/opt', '/dev', '/dev/pts', '/home', '/boot', '/dev/shm']

if os.name == 'nt':
    close_fds = False
else:
    close_fds = True

def bdecode(buff):
    if sys.version_info[0] < 3:
        return buff
    else:
        try:
            return str(buff, "utf-8")
        except:
            return str(buff, "ascii")
    return buff

def is_string(s):
    """ python[23] compatible
    """
    if sys.version_info[0] == 2:
        l = (str, unicode)
    else:
        l = (str)
    if isinstance(s, l):
        return True
    return False

def mimport(*args, fallback=True):
    def fmt(s):
        if len(s) > 1:
            return s[0].upper()+s[1:].lower()
        elif len(s) == 1:
            return s[0].upper()
        else:
            return ""

    mod = ""
    for i, e in enumerate(*args):
        if e in ("res", "prov") and i == 0:
            mod += e
        else:
            mod += fmt(e)

    try:
        return __import__(mod+rcEnv.sysname)
    except ImportError:
        pass

    try:
        return __import__(mod)
    except ImportError:
        pass

    if fallback and len(args) > 1:
        args = args[:-1]
        return mimport(*args, fallback=fallback)
    else:
        raise ImportError("no module found")

def ximport(base):
    mod = base + rcEnv.sysname
    try:
        m = __import__(mod)
        return m
    except:
        pass

    return __import__(base)

def check_privs():
    if os.name == 'nt':
        return
    if os.getuid() != 0:
        import copy
        l = copy.copy(sys.argv)
        l[0] = os.path.basename(l[0]).replace(".py", "")
        print('Insufficient privileges. Try:\n sudo ' + ' '.join(l))
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
    if program is None:
        return
    def ext_candidates(fpath):
        yield fpath
        for ext in os.environ.get("PATHEXT", "").split(os.pathsep):
            yield fpath + ext

    fpath, fname = os.path.split(program)
    if fpath:
        if os.path.isfile(program) and is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            for candidate in ext_candidates(exe_file):
                if is_exe(candidate):
                    return candidate

    return

def justcall(argv=['/bin/false']):
    """subprosses call argv, return (stdout,stderr,returncode)
    """
    if which(argv[0]) is None:
        return ("", "", 1)
    process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=close_fds)
    stdout, stderr = process.communicate(input=None)
    return bdecode(stdout), bdecode(stderr), process.returncode

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
         err_to_info=False,
         warn_to_info=False):
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
        process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=close_fds)
        buff = process.communicate()
        buff = tuple(map(lambda x: bdecode(x), buff))
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
            elif warn_to_info:
                log.info('command successful but stderr:\n' + buff[1])
            else:
                log.warning('command successful but stderr:\n' + buff[1])
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
    process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=close_fds)
    process.wait()
    return process.returncode

def vcall(argv=['/bin/false'],
          log=None,
          err_to_warn=False,
          err_to_info=False,
          warn_to_info=False):
    return call(argv,
                log=log,
                info=True,
                outlog=True,
                err_to_warn=err_to_warn,
                err_to_info=err_to_info,
                warn_to_info=warn_to_info)

def getmount(path):
    path = os.path.abspath(path)
    while path != os.path.sep:
        if not os.path.islink(path) and os.path.ismount(path):
            return path
        path = os.path.abspath(os.path.join(path, os.pardir))
    return path

def protected_mount(path):
    if getmount(path) in protected_dirs:
        return True
    return False

def printplus(obj):
    """
    Pretty-prints the object passed in.

    """
    # Dict
    if isinstance(obj, dict):
        for k, v in sorted(obj.items()):
            print("%s: %s" % (str(k), str(v)))

    # List or tuple
    elif isinstance(obj, list) or isinstance(obj, tuple):
        for x in obj:
            print(x)

    # Other
    else:
        print(obj)

def cmdline2list(cmdline):
    """
    Translate a command line string into a list of arguments, using
    using the same rules as the MS C runtime:

    1) Arguments are delimited by white space, which is either a
       space or a tab.

    2) A string surrounded by double quotation marks is
       interpreted as a single argument, regardless of white space
       contained within.  A quoted string can be embedded in an
       argument.

    3) A double quotation mark preceded by a backslash is
       interpreted as a literal double quotation mark.

    4) Backslashes are interpreted literally, unless they
       immediately precede a double quotation mark.

    5) If backslashes immediately precede a double quotation mark,
       every pair of backslashes is interpreted as a literal
       backslash.  If the number of backslashes is odd, the last
       backslash escapes the next double quotation mark as
       described in rule 3.
    """

    # See
    # http://msdn.microsoft.com/library/en-us/vccelng/htm/progs_12.asp

    # Step 1: Translate all literal quotes into QUOTE.  Justify number
    # of backspaces before quotes.
    tokens = []
    bs_buf = ""
    QUOTE = 1 # \", literal quote
    for c in cmdline:
        if c == '\\':
            bs_buf += c
        elif c == '"' and bs_buf:
            # A quote preceded by some number of backslashes.
            num_bs = len(bs_buf)
            tokens.extend(["\\"] * (num_bs//2))
            bs_buf = ""
            if num_bs % 2:
                # Odd.  Quote should be placed literally in array
                tokens.append(QUOTE)
            else:
                # Even.  This quote serves as a string delimiter
                tokens.append('"')

        else:
            # Normal character (or quote without any preceding
            # backslashes)
            if bs_buf:
                # We have backspaces in buffer.  Output these.
                tokens.extend(list(bs_buf))
                bs_buf = ""

            tokens.append(c)

    # Step 2: split into arguments
    result = [] # Array of strings
    quoted = False
    arg = [] # Current argument
    tokens.append(" ")
    for c in tokens:
        if c == '"':
            # Toggle quote status
            quoted = not quoted
        elif c == QUOTE:
            arg.append('"')
        elif c in (' ', '\t'):
            if quoted:
                arg.append(c)
            else:
                # End of argument.  Output, if anything.
                if arg:
                    result.append(''.join(arg))
                    arg = []
        else:
            # Normal character
            arg.append(c)

    return result

def try_decode(string, codecs=['utf8', 'latin1']):
    for i in codecs:
        try:
            return string.decode(i)
        except:
            pass
    return string

def getaddr_cache_set(name, addr):
    cache_d = os.path.join(rcEnv.pathvar, "cache", "addrinfo")
    if not os.path.exists(cache_d):
        os.makedirs(cache_d)
    cache_f = os.path.join(cache_d, name)
    with open(cache_f, 'w') as f:
        f.write(addr)
    return addr

def getaddr_cache_get(name):
    cache_d = os.path.join(rcEnv.pathvar, "cache", "addrinfo")
    if not os.path.exists(cache_d):
        os.makedirs(cache_d)
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

def convert_size(s, _to='', _round=1):
    l = ['', 'K', 'M', 'G', 'T', 'P', 'Z', 'E']
    if type(s) in (int, float):
        s = str(s)
    s = s.strip().replace(",", ".")
    if len(s) == 0:
        return 0
    if s == '0':
        return 0
    size = s
    unit = ""
    for i, c in enumerate(s):
        if not c.isdigit() and c != '.':
            size = s[:i]
            unit = s[i:].strip()
            break
    if 'i' in unit:
        factor = 1000
    else:
        factor = 1024
    if len(unit) > 0:
        unit = unit[0].upper()
    size = float(size)

    try:
        start_idx = l.index(unit)
    except:
        raise Exception("unsupported unit in converted value: %s" % s)

    for i in range(start_idx):
        size *= factor

    if 'i' in _to:
        factor = 1000
    else:
        factor = 1024
    if len(_to) > 0:
        unit = _to[0].upper()
    else:
        unit = ''

    if unit == 'B':
        unit = ''

    try:
        end_idx = l.index(unit)
    except:
        raise Exception("unsupported target unit: %s" % unit)

    for i in range(end_idx):
        size /= factor

    size = int(size)
    d = size % _round
    if d > 0:
        size = (size // _round) * _round
    return size

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
    if '.' in s:
        return dotted_to_cidr(s)
    elif re.match("^(0x)*[0-9a-f]{8}$", s):
        # example: 0xffffff00
        s = hexmask_to_dotted(s)
        return dotted_to_cidr(s)
    return s

if __name__ == "__main__":
    #print("call(('id','-a'))")
    #(r,output,err)=call(("/usr/bin/id","-a"))
    #print("status: ", r, "output:", output)
    print(convert_size("10000 KiB", _to='MiB', _round=3))
    print(convert_size("10M", _to='', _round=4096))

