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

import svcBuilder
import rcStatus
import rcColor
from svcmgr_parser import SvcmgrOptParser
import rcExceptions as ex
from rcUtilities import ximport, check_privs
from rcGlobalEnv import Storage

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

def get_minimal(action, options):
    """
    Return True if the services can be built with minimal parsing
    """
    if action in ("set", "unset"):
        return True
    if action == "ls" and not options.status:
        return True
    if action == "get" and not options.eval:
        return True
    if action == "edit_config":
        return True
    if action == "delete":
        if options.unprovision:
            return False
        else:
            return True
    if action.startswith("print_config"):
        return True
    if action.startswith("json_config"):
        return True
    if action.startswith("collector_"):
        return True
    return False

def get_build_kwargs(optparser, options, action):
    """
    Return the service build function keyword arguments, deduced from
    parsed command line options.
    """
    build_kwargs = {}
    build_kwargs["minimal"] = get_minimal(action, options)

    if len(set(["svcnames", "status"]) & set(build_kwargs.keys())) == 0:
        if os.environ.get("OSVC_SERVICE_LINK"):
            build_kwargs["svcnames"] = [os.environ.get("OSVC_SERVICE_LINK")]
        if hasattr(options, "svcs") and options.svcs is not None:
            build_kwargs["svcnames"] = options.svcs

    if hasattr(options, "status") and options.status is not None:
        build_kwargs["status"] = [rcStatus.status_value(s) for s in options.status.split(",")]

    build_kwargs["create_instance"] = action in ("create", "pull")

    return build_kwargs

def do_svcs_action_detached(argv=None):
    """
    Executes the services action in detached process mode, so that
    a term/kill signal on the parent process does not abort the action.

    Keyboard interrupts do abort the detached process though.
    """
    ret = 0
    try:
        import subprocess
        import signal
        proc = subprocess.Popen([sys.executable, __file__] + argv + ["--daemon"],
                                stdout=None, stderr=None, stdin=None,
                                close_fds=True, cwd=os.sep,
                                preexec_fn=os.setsid)
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

    if not options.daemon and ( \
        action in ("stop", "shutdown", "unprovision", "switch") or \
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

def do_svc_create(node, svcnames, action, options, build_kwargs):
    """
    Handle service creation command.
    """
    ret = 0
    try:
        node.install_service(svcnames, fpath=options.config,
                             template=options.template)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        ret = 1

    if options.config is None and options.template is None:
        data = getattr(svcBuilder, action)(svcnames, options.resource,
                                           interactive=options.interactive,
                                           provision=options.provision)
    else:
        data = {"rid": [], "ret": 0}

    # if the user want to provision a resource defined via configuration
    # file edition, he will set --rid <rid> or --tag or --subset to point
    # the update command to it
    options.rid = ",".join(data.get("rid", []))

    # force a refresh of node.svcs
    try:
        node.rebuild_services(svcnames, build_kwargs["minimal"])
    except ex.excError as exc:
        print(exc, file=sys.stderr)
        ret = 1

    if len(node.svcs) == 1 and (options.config or options.template):
        node.svcs[0].setenv(options.env, options.interactive)
        # setenv changed the service config file
        # we need to rebuild again
        try:
            node.rebuild_services(svcnames, build_kwargs["minimal"])
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            ret = 1

    if options.provision:
        if len(node.svcs) == 1 and ( \
            options.config or \
            options.template \
           ):
            node.svcs[0].action("provision", options)

    if ret != 0:
        return ret

    return data["ret"]

def prepare_options(options):
    """
    Prepare and return the options Storage() as expected by the Svc::action
    and Node::do_svcs_action methods.
    """
    opts = Storage()
    for key, val in options.__dict__.items():
        opts[key.replace("parm_", "")] = val
    return opts

def split_env(arg):
    idx = arg.index("=")
    option = arg[:idx]
    value = arg[idx+1:]
    return option, value

def export_env_from_options(options):
    for arg in options.get("env", []):
        option, value = split_env(arg)
        option = option.upper()
        os.environ[option] = value

def _main(node, argv=None):
    """
    Build the service list, full or minimal depending on the requested action.
    Execute action-specific codepaths.
    """
    build_err = False
    svcnames = []
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
    except AttributeError:
        pass
    if os.environ.get("OSVC_SERVICE_LINK") is None and \
       action != "ls" and options.svcs is None and options.status is None:
        raise ex.excError("no service specified. set --service or --status.")
    if action != "create":
        expanded_svcs = node.svcs_selector(options.svcs)
        if options.svcs in (None, "*") and expanded_svcs == []:
            return
        options.svcs = expanded_svcs
    else:
        options.svcs = options.svcs.split(",")
    node.options.single_service = options.svcs is not None and \
                                  len(options.svcs) == 1

    build_kwargs = get_build_kwargs(optparser, options, action)

    if action != "create":
        try:
            node.build_services(**build_kwargs)
        except IOError as exc:
            if len(str(exc)) > 0:
                print(exc, file=sys.stderr)
            build_err = True
        except ex.excError as exc:
            if len(str(exc)) > 0:
                print(exc, file=sys.stderr)
            build_err = True

    if node.svcs is not None and len(node.svcs) > 0:
        svcnames = [svc.svcname for svc in node.svcs]
    elif action == "create" and "svcnames" in build_kwargs:
        svcnames = build_kwargs["svcnames"]

    if len(svcnames) == 0:
        if action == "ls":
            return
        if not build_err:
            sys.stderr.write("No service specified. Try:\n"
                             " svcmgr -s <svcname>[,<svcname>]\n"
                             " svcmgr --status <status>[,<status>]\n"
                             " <svcname>\n")
        return 1

    if action == "create":
        return do_svc_create(node, svcnames, action, options, build_kwargs)

    node.set_rlimit()
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

    try:
        ret = _main(node, argv=argv)
    except ex.excError as exc:
        print(exc, file=sys.stderr)
        return 1
    except IOError as exc:
        if exc.errno == errno.EACCES:
            check_privs()
            return 1
        else:
            raise
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
