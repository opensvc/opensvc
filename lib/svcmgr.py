"""
This executable is wrapped by the opensvc shell script.

It's the entrypoint for all OpenSVC services management ops.
"""
from __future__ import print_function
import sys
import os

import svcBuilder
import rcStatus
import rcColor
import svcmgr_parser
import rcExceptions as ex
from rcUtilities import ximport


def refresh_node_svcs(node, svcnames, minimal):
    """
    Delete the list of Svc objects in the Node object and create a new one.

    Args:
      svcnames: add only Svc objects for services specified
      minimal: include a minimal set of properties in the new Svc objects
    """
    del node.svcs
    node.svcs = None
    node.build_services(svcnames=svcnames, autopush=False, minimal=minimal)

def get_docker_argv():
    """
    Extract docker argv from svcmgr argv.

    svcmgr acts as a wrapper for docker, setting the service-specific socket
    if necessary.
    """
    if len(sys.argv) < 2:
        return
    if 'docker' not in sys.argv:
        return
    pos = sys.argv.index('docker')
    if len(sys.argv) > pos + 1:
        docker_argv = sys.argv[pos+1:]
    else:
        docker_argv = []
    sys.argv = sys.argv[:pos+1]
    return docker_argv

def get_minimal(action, options):
    """
    Return True if the services can be built with minimal parsing
    """
    if action in ("set", "unset"):
        return True
    if action == "get" and not options.eval:
        return True
    if action == "edit_config":
        return True
    if action == "delete":
       if not options.unprovision:
           return True
       elif not options.parm_rid and \
            not options.parm_tags and \
            not options.parm_subsets:
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
        if hasattr(options, "parm_svcs") and options.parm_svcs is not None:
            build_kwargs["svcnames"] = options.parm_svcs.split(',')

    if hasattr(options, "parm_status") and options.parm_status is not None:
        build_kwargs["status"] = [rcStatus.status_value(s) for s in options.parm_status.split(",")]

    if hasattr(options, "parm_primary") and options.parm_primary is not None and \
       hasattr(options, "parm_secondary") and options.parm_secondary is not None:
        optparser.parser.error("--onlyprimary and --onlysecondary are exclusive")

    if hasattr(options, "parm_primary") and options.parm_primary is not None:
        build_kwargs["onlyprimary"] = options.parm_primary

    if hasattr(options, "parm_secondary") and options.parm_secondary is not None:
        build_kwargs["onlysecondary"] = options.parm_secondary

    # don't autopush when the intent is to push explicitely
    if action == "push":
        build_kwargs["autopush"] = False
    else:
        build_kwargs["autopush"] = True

    return build_kwargs

def set_svcs_options(node, options, docker_argv):
    """
    Relay some properties extracted from the command line as Svc object
    properties.
    """
    if options.slave is not None:
        slave = options.slave.split(',')
    else:
        slave = None

    for svc in node.svcs:
        svc.options = options
        svc.force = options.force
        svc.remote = options.remote
        svc.cron = options.cron
        svc.options.slaves = options.slaves
        svc.options.slave = slave
        svc.options.master = options.master
        svc.options.recover = options.recover
        svc.options.discard = options.discard
        svc.cluster = options.cluster
        svc.destination_node = options.parm_destination_node
        if docker_argv is not None:
            svc.docker_argv = docker_argv

def do_svcs_action_detached():
    """
    Executes the services action in detached process mode, so that
    a term/kill signal on the parent process does not abort the action.

    Keyboard interrupts do abort the detached process though.
    """
    ret = 0
    try:
        import subprocess
        import signal
        proc = subprocess.Popen([sys.executable] + sys.argv + ["--daemon"],
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

def do_svcs_action(node, options, action):
    """
    Execute the services action, switching between detached mode for
    stop*/shutdown/unprovision/switch, and inline mode for other actions.
    """
    ret = 0
    rid, tags, subsets = get_specifiers(options)

    if not options.daemon and ( \
        action.startswith("stop") or \
        action in ("shutdown", "unprovision", "switch") \
       ):
        ret = do_svcs_action_detached()
    else:
        try:
            ret = node.do_svcs_action(action, rid=rid, tags=tags,
                                      subsets=subsets)
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            ret = 1
    return ret

def get_specifiers(options):
    """
    Extract rid, tags, subsets specifiers from the commandline options.
    Return them as a tuple of lists.
    """
    if hasattr(options, "parm_rid") and options.parm_rid is not None:
        rid = options.parm_rid.split(',')
    else:
        rid = []

    if options.parm_tags is not None:
        tags = options.parm_tags.replace("+", ",+").split(',')
    else:
        tags = []

    if options.parm_subsets is not None:
        subsets = options.parm_subsets.split(',')
    else:
        subsets = []
    return rid, tags, subsets

def do_svc_create_or_update(node, svcnames, action, options, build_kwargs):
    """
    Handle service creation or update commands.
    """
    ret = 0
    rid, tags, subsets = get_specifiers(options)

    if action == 'update' or (action == 'create' and \
       options.param_config is None and options.param_template is None):
        data = getattr(svcBuilder, action)(svcnames, options.resource,
                                           interactive=options.interactive,
                                           provision=options.provision)
    else:
        data = {"rid": [], "ret": 0}

    # if the user want to provision a resource defined via configuration
    # file edition, he will set --rid <rid> or --tag or --subset to point
    # the update command to it
    rid += data.get("rid", [])

    # force a refresh of node.svcs
    # don't push to the collector yet
    try:
        refresh_node_svcs(node, svcnames, build_kwargs["minimal"])
    except ex.excError as exc:
        print(exc, file=sys.stderr)
        ret = 1

    if len(node.svcs) == 1 and ( \
        options.param_config or \
        options.param_template \
       ):
        node.svcs[0].setenv(options.env, options.interactive)
        # setenv changed the service config file
        # we need to rebuild again
        try:
            refresh_node_svcs(node, svcnames, build_kwargs["minimal"])
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            ret = 1

    if options.provision:
        if len(node.svcs) == 1 and ( \
            len(rid) > 0 or \
            options.param_config or \
            options.param_template \
           ):
            node.svcs[0].action("provision", rid=rid, tags=tags,
                                subsets=subsets)

    if ret != 0:
        return ret

    return data["ret"]

def _main(node):
    """
    Do to many things
    """
    build_err = False
    svcnames = []
    ret = 0

    docker_argv = get_docker_argv()
    optparser = svcmgr_parser.OptParser()
    options, args = optparser.parser.parse_args()
    rcColor.use_color = options.color
    try:
        node.options.format = options.format
    except AttributeError:
        pass

    action = optparser.get_action_from_args(args, options)
    build_kwargs = get_build_kwargs(optparser, options, action)

    if action not in ("create", "pull"):
        try:
            node.build_services(**build_kwargs)
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            build_err = True

    if node.svcs is not None and len(node.svcs) > 0:
        svcnames = [svc.svcname for svc in node.svcs]
    elif action in ("create", "pull") and "svcnames" in build_kwargs:
        svcnames = build_kwargs["svcnames"]

    if len(svcnames) == 0:
        if not build_err:
            sys.stderr.write("No service specified. Try:\n"
                             " svcmgr -s <svcname>[,<svcname>]\n"
                             " svcmgr --status <status>[,<status>]\n"
                             " <svcname>\n")
        return 1

    if action == 'pull' and (node.svcs is None or len(node.svcs) == 0):
        return node.pull_services(svcnames)

    if action == "create":
        try:
            node.install_service(svcnames, cf=options.param_config,
                                 template=options.param_template)
            ret = 0
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            ret = 1

    if action in ['create', 'update']:
        return do_svc_create_or_update(node, svcnames, action, options,
                                       build_kwargs)

    node.options.parallel = options.parallel
    node.options.waitlock = options.parm_waitlock

    node.set_rlimit()
    set_svcs_options(node, options, docker_argv)
    ret = do_svcs_action(node, options, action)

    try:
        import logging
        logging.shutdown()
    except:
        pass

    return ret

def main():
    """
    Instanciate a Node object.
    Call the real deal making sure the node is finally freed.
    """
    node_mod = ximport('node')
    try:
        node = node_mod.Node()
    except Exception as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)

    try:
        ret = _main(node)
    except KeyboardInterrupt:
        ret = 1
    finally:
        node.close()

    sys.exit(ret)


if __name__ == "__main__":
    main()
