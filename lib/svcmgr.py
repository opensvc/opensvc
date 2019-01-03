# coding: utf8

"""
This executable is wrapped by the opensvc shell script.

It's the entrypoint for all OpenSVC services management ops.
"""
from __future__ import print_function
from __future__ import absolute_import

import sys
import os
import errno

import rcStatus
import rcColor
from svcmgr_parser import SvcmgrOptParser
import rcExceptions as ex
from rcUtilities import ximport, check_privs, svcpath_from_link, \
                        check_svclink_ns, fmt_svcpath
from rcGlobalEnv import rcEnv
from storage import Storage

def get_docker_argv(argv=None):
    """
    Extract docker argv from svcmgr argv.

    svcmgr acts as a wrapper for docker, setting the service-specific socket
    if necessary.
    """
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) < 2:
        return argv, []
    if "docker" not in argv:
        return argv, []
    pos = argv.index('docker')
    if len(argv) > pos + 1:
        docker_argv = argv[pos+1:]
    else:
        docker_argv = []
    argv = argv[:pos+1]
    return argv, docker_argv

def get_build_kwargs(optparser, options, action):
    """
    Return the service build function keyword arguments, deduced from
    parsed command line options.
    """
    build_kwargs = {}

    if len(set(["svcpaths", "status"]) & set(build_kwargs.keys())) == 0:
        if hasattr(options, "svcs") and options.svcs is not None:
            build_kwargs["svcpaths"] = options.svcs

    if hasattr(options, "status") and options.status is not None:
        build_kwargs["status"] = [rcStatus.status_value(s) for s in options.status.split(",")]

    build_kwargs["create_instance"] = action in ("create", "pull")

    return build_kwargs

def expand_svcs(options, node):
    # selection trough symlink to svcmgr
    svclink = os.environ.get("OSVC_SERVICE_LINK")
    if svclink:
        try:
            check_svclink_ns(svclink, options.namespace)
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            return []
        return [svcpath_from_link(svclink)]
    return node.svcs_selector(options.svcs, options.namespace)

def do_svcs_action_detached(argv=None):
    """
    Executes the services action in detached process mode, so that
    a term/kill signal on the parent process does not abort the action.

    Keyboard interrupts do abort the detached process though.
    """
    ret = 0
    env = {}
    env.update(os.environ)
    env["OSVC_DETACHED"] = "1"
    env["OSVC_PARENT_SESSION_UUID"] = rcEnv.session_uuid
    try:
        import subprocess
        import signal
        kwargs = {}
        try:
            kwargs["preexec_fn"] = os.setsid
        except AttributeError:
            pass
        proc = subprocess.Popen([sys.executable, os.path.abspath(__file__)] + argv,
                                stdout=None, stderr=None, stdin=None,
                                close_fds=True, cwd=os.sep,
                                env=env, **kwargs)
        proc.wait()
        ret = proc.returncode
    except KeyboardInterrupt as exc:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        print("kill detached process")
        ret = 1
    except ex.excSignal as exc:
        print("the action, detached as pid %d, "
              "will continue executing" % proc.pid)
        ret = 1
    except Exception as exc:
        print(exc, file=sys.stderr)
        ret = 1
    return ret

def do_svcs_action(node, options, action, argv):
    """
    Execute the services action, switching between detached mode for
    stop*/shutdown/unprovision/switch, and inline mode for other actions.
    """
    ret = 0

    if os.environ.get("OSVC_ACTION_ORIGIN") != "daemon" and \
       os.environ.get("OSVC_DETACHED") != "1" and ( \
        action in ("stop", "shutdown", "unprovision") or \
        (action == "delete" and options.unprovision == True)
       ):
        ret = do_svcs_action_detached(argv)
    else:
        try:
            ret = node.do_svcs_action(action, options)
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            ret = 1
    return ret

def prepare_options(options):
    """
    Prepare and return the options Storage() as expected by the Svc::action
    and Node::do_svcs_action methods.
    """
    opts = Storage()
    for key, val in options.__dict__.items():
        opts[key.replace("parm_", "")] = val
    try:
        namespace = options.namespace
    except AttributeError:
        # svclink parser doesn't include the namespace option
        namespace = None
    if namespace:
        opts.namespace = namespace
    elif "OSVC_NAMESPACE" in os.environ:
        opts.namespace = os.environ["OSVC_NAMESPACE"]
    return opts

def split_env(arg):
    idx = arg.index("=")
    option = arg[:idx]
    value = arg[idx+1:]
    return option, value

def export_env_from_options(options):
    if options.get("daemon"):
        os.environ["OSVC_DETACHED"] = "1"
    for arg in options.get("env", []):
        option, value = split_env(arg)
        option = option.upper()
        os.environ[option] = value

def _main(node, argv=None):
    """
    Build the service list.
    Execute action-specific codepaths.
    """
    build_err = False
    svcpaths = []
    ret = 0

    argv, docker_argv = get_docker_argv(argv)
    optparser = SvcmgrOptParser()
    options, action = optparser.parse_args(argv)
    options = prepare_options(options)
    export_env_from_options(options)
    options.docker_argv = docker_argv
    rcColor.use_color = options.color
    try:
        node.options.format = options.format
        node.options.jsonpath_filter = options.jsonpath_filter
    except AttributeError:
        pass
    if os.environ.get("OSVC_SERVICE_LINK") is None and \
       action != "ls" and options.svcs is None and options.status is None:
        raise ex.excError("no service specified. set --service or --status.")
    if action != "create":
        expanded_svcs = expand_svcs(options, node)
        if options.svcs in (None, "*") and expanded_svcs == []:
            return
        options.svcs = expanded_svcs
    else:
        options.svcs = options.svcs.split(",")

    node.set_rlimit()
    build_kwargs = get_build_kwargs(optparser, options, action)

    if action != "create":
        try:
            node.build_services(**build_kwargs)
        except ex.excError as exc:
            if len(str(exc)) > 0:
                print(exc, file=sys.stderr)
            build_err = True

    if node.svcs is not None and len(node.svcs) > 0:
        svcpaths = [svc.svcpath for svc in node.svcs]
    elif action == "create" and "svcpaths" in build_kwargs:
        svcpaths = build_kwargs["svcpaths"]

    if len(svcpaths) == 0:
        if action == "ls":
            return
        if not build_err:
            sys.stderr.write("No service specified.\n"
                             "Syntax:\n"
                             " svcmgr -s <svc selector> [--namespace <ns>]\n")
        return 1

    if action == "create":
        return node.create_service(svcpaths, options)

    ret = do_svcs_action(node, options, action, argv=argv)

    try:
        import logging
        logging.shutdown()
    except:
        pass

    return ret

def main(argv=None):
    """
    Instanciate a Node object.
    Call the real deal making sure the node is finally freed.
    """
    ret = 0
    node_mod = ximport('node')
    try:
        node = node_mod.Node()
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1

    check_privs()

    try:
        ret = _main(node, argv=argv)
    except ex.excError as exc:
        print(exc, file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 1
    finally:
        node.close()

    if ret is None:
        ret = 0

    return ret

if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
