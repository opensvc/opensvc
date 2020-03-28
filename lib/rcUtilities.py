from __future__ import print_function

import glob
import importlib
import locale
import os
import re
import shlex
import sys
from itertools import chain

import six

import core.exceptions as ex
from core.contexts import want_context
from rcGlobalEnv import rcEnv
from utilities.proc import call, which
from utilities.string import is_string

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

def check_privs():
    if "OSVC_CONTEXT" in os.environ or "OSVC_CLUSTER" in os.environ:
        return
    if os.name == 'nt':
        return
    if os.geteuid() == 0:
        return
    print("Insufficient privileges", file=sys.stderr)
    sys.exit(1)


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
        raise ex.Error(str(exc))

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
    # raise ex.Error("can not set a C lang with utf8 encoding")
    os.environ["LANG"] = "C"
    os.environ["LC_NUMERIC"] = "C"
    os.environ["LC_TIME"] = "C"


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
        if not path or path[-1] == os.sep:
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
        raise ex.Error("invalid name '%s'. names must not clash with kinds"
                          " %s." % (name, ", ".join(rcEnv.kinds)))
    if re.match(VALID_NAME_RFC952, name):
        return
    raise ex.Error("invalid name '%s'. names must contain only dots, letters, "
                      "digits and hyphens, start with a letter and end with "
                      "a digit or letter (rfc 952)." % name)


def factory(kind):
    """
    Return a Svc or Node object
    """
    if kind == "node":
        from core.node import Node
        return Node
    try:
        mod = importlib.import_module("core.objects."+kind)
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


def create_protected_file(filepath, buff, mode):
    with open(filepath, mode) as f:
        if os.name == 'posix':
            os.chmod(filepath, 0o0600)
        f.write(buff)



