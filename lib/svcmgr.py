from __future__ import print_function
import sys
import os
import optparse
import platform

#
# add project lib to path
#
prog = "svcmgr"

import svcBuilder
import rcStatus
import rcColor
import rcOptParser
import rcExceptions as ex
from rcUtilities import ximport
from rcGlobalEnv import rcEnv
node_mod = ximport('node')

sysname, nodename, x, x, machine, x = platform.uname()
build_err = False

try:
    from version import version
except:
    version = "dev"

def _exit(r):
    node.close()
    if build_err:
        sys.exit(1)
    sys.exit(r)

try:
    node = node_mod.Node()
except Exception as e:
    print(e, file=sys.stderr)
    sys.exit(1)


__ver = prog + " version " + version
__usage = prog + " [ OPTIONS ] COMMAND\n\n"
parser = optparse.OptionParser(version=__ver, usage=__usage + rcOptParser.format_desc())
parser.add_option("--eval", default=False, action="store_true", dest="eval",
                  help="If set with the 'get' action, the printed value of --param is scoped and dereferenced.")
parser.add_option("--daemon", default=False, action="store_true", dest="daemon",
                  help="a flag inhibiting the daemonization. set by the daemonization routine.")
parser.add_option("--color", default="auto", action="store", dest="color",
                  help="colorize output. possible values are : auto=guess based on tty presence, always|yes=always colorize, never|no=never colorize")
parser.add_option("--debug", default=False, action="store_true", dest="debug",
                  help="debug mode")
parser.add_option("--recover", default=False, action="store_true", dest="recover",
                  help="Recover the stashed erroneous configuration file in a 'edit config' command")
parser.add_option("--discard", default=False, action="store_true", dest="discard",
                  help="Discard the stashed erroneous configuration file in a 'edit config' command")
parser.add_option("--dry-run", default=False, action="store_true", dest="dry_run",
                  help="Show the action execution plan")
parser.add_option("--disable-rollback", default=False, action="store_true", dest="disable_rollback",
                  help="Exit without resource activation rollback on start action error")
parser.add_option("-p", "--parallel", default=False, action="store_true", dest="parallel",
                  help="start actions on specified services in parallel")
parser.add_option("--ignore-affinity", default=False, action="store_true", dest="ignore_affinity",
                  help="ignore service anti-affinity with other services check")
parser.add_option("--remote", default=False, action="store_true", dest="remote",
                  help="flag action as triggered by a remote node. used to avoid recursively triggering actions amongst nodes")
parser.add_option("-f", "--force", default=False, action="store_true", dest="force",
                  help="force action, ignore sanity check warnings")
parser.add_option("--cron", default=False, action="store_true", dest="cron",
                  help="used by cron'ed action to tell the collector to treat the log entries as such")
parser.add_option("--slaves", default=False, action="store_true", dest="slaves",
                  help="option to set to limit the action scope to all slave service resources")
parser.add_option("--slave", default=None, action="store", dest="slave",
                  help="option to set to limit the action scope to the service resources in the specified, comma-sperated, slaves")
parser.add_option("--master", default=False, action="store_true", dest="master",
                  help="option to set to limit the action scope to the master service resources")
parser.add_option("-c", "--cluster", default=False, action="store_true", dest="cluster",
                  help="option to set when excuting from a clusterware to disable safety net")
parser.add_option("-i", "--interactive", default=False, action="store_true", dest="interactive",
                  help="prompt user for a choice instead of going for defaults or failing")
parser.add_option("--rid", default=None, action="store", dest="parm_rid",
                  help="comma-separated list of resource to limit action to")
parser.add_option("--subsets", default=None, action="store", dest="parm_subsets",
                  help="comma-separated list of resource subsets to limit action to")
parser.add_option("--tags", default=None, action="store", dest="parm_tags",
                  help="comma-separated list of resource tags to limit action to. The + separator can be used to impose multiple tag conditions. Example: tag1+tag2,tag3 limits the action to resources with both tag1 and tag2, or tag3.")
parser.add_option("--resource", default=[], action="append",
                  help="a resource definition in json dictionary format fed to create or update")
parser.add_option("--provision", default=False, action="store_true", dest="provision",
                  help="with the install or create actions, provision the service resources after config file creation. defaults to False.")
parser.add_option("--unprovision", default=False, action="store_true", dest="unprovision",
                  help="with the delete action, unprovision the service resources before config files file deletion. defaults to False.")
parser.add_option("--env", default=[], action="append", dest="env",
                  help="with the create action, set a env section parameter. multiple --env <key>=<val> can be specified.")
parser.add_option("--waitlock", default=60, action="store", dest="parm_waitlock", type="int",
                  help="comma-separated list of resource tags to limit action to")
parser.add_option("--to", default=None, action="store", dest="parm_destination_node",
                  help="remote node to start or migrate the service to")
parser.add_option("--show-disabled", default=None,
                  action="store_true", dest="show_disabled",
                  help="tell print|json status action to include the disabled resources in the output, irrespective of the show_disabled service configuration setting.")
parser.add_option("--hide-disabled", default=None,
                  action="store_false", dest="show_disabled",
                  help="tell print|json status action to not include the disabled resources in the output, irrespective of the show_disabled service configuration setting.")
parser.add_option("--attach", default=False,
                  action="store_true", dest="attach",
                  help="attach the modulesets specified during a compliance check/fix/fixable command")
parser.add_option("--module", default="",
                  action="store", dest="module",
                  help="compliance, set module list")
parser.add_option("--moduleset", default="",
                  action="store", dest="moduleset",
                  help="compliance, set moduleset list. The 'all' value can be used in conjonction with detach.")
parser.add_option("--ruleset", default="",
                  action="store", dest="ruleset",
                  help="compliance, set ruleset list. The 'all' value can be used in conjonction with detach.")
parser.add_option("--ruleset-date", default="",
                  action="store", dest="ruleset_date",
                  help="compliance, use rulesets valid on specified date")
parser.add_option("--param", default=None,
                  action="store", dest="param",
                  help="point a service configuration parameter for the 'get' and 'set' actions")
parser.add_option("--value", default=None,
                  action="store", dest="value",
                  help="set a service configuration parameter value for the 'set --param' action")
parser.add_option("--duration", default=None, action="store", dest="duration", type="int",
                  help="a duration expressed in minutes. used with the 'collector ack unavailability' action")
parser.add_option("--account", default=False, action="store_true", dest="account",
                  help="decides that the unavailabity period should be deduced from the service availability anyway. used with the 'collector ack unavailability' action")
parser.add_option("--begin", default=None, action="store", dest="begin",
                  help="a begin date expressed as 'YYYY-MM-DD hh:mm'. used with the 'collector ack unavailability' action")
parser.add_option("--end", default=None, action="store", dest="end",
                  help="a end date expressed as 'YYYY-MM-DD hh:mm'. used with the 'collector ack unavailability' action")
parser.add_option("--comment", default=None, action="store", dest="comment",
                  help="a comment to log when used with the 'collector ack unavailability' action")
parser.add_option("--author", default=None, action="store", dest="author",
                  help="the acker name to log when used with the 'collector ack unavailability' action")
parser.add_option("--id", default=0, action="store", dest="id", type="int",
                  help="specify an id to act on")
parser.add_option("--refresh", default=False, action="store_true", dest="refresh",
                  help="drop last resource status cache and re-evaluate before printing with the 'print [json] status' commands")
parser.add_option("--verbose", default=False, action="store_true", dest="verbose",
                  help="add more information to some print commands: +next in 'print schedule'")
parser.add_option("--tag", default=None,
                  action="store", dest="tag",
                  help="a tag specifier used by 'collector create tag', 'collector add tag', 'collector del tag'")
parser.add_option("--like", default="%",
                  action="store", dest="like",
                  help="a sql like filtering expression. leading and trailing wildcards are automatically set.")
parser.add_option("--format", default=None,
                  action="store", dest="format",
                  help="specify a data formatter for output of the print* and collector* commands. possible values are json or table.")



cmd = os.path.basename(__file__)

if cmd in ('svcmgr', 'svcmgr.py', None):
    parser.add_option("-s", "--service", default=None, action="store", dest="parm_svcs",
              help="comma-separated list of service to operate on")
    parser.add_option("--status", default=None, action="store", dest="parm_status",
              help="operate only on service in the specified status (up/down/warn)")
    parser.add_option("--onlyprimary", default=None, action="store_true", dest="parm_primary",
              help="operate only on service flagged for autostart on this node")
    parser.add_option("--onlysecondary", default=None, action="store_true", dest="parm_secondary",
              help="operate only on service not flagged for autostart on this node")
    parser.add_option("--config", default=None, action="store", dest="param_config",
              help="the configuration file to use when creating or installing a service")
    parser.add_option("--template", default=None, action="store", dest="param_template",
              help="the configuration file template name or id, served by the collector, to use when creating or installing a service")

def refresh_node_svcs(svcnames, minimal):
    del(node.svcs)
    node.svcs = None
    try:
        node.build_services(svcnames=svcnames, autopush=False, minimal=minimal)
    except ex.excError as e:
        print(e, file=sys.stderr)
        return 1

def main():
    action = None
    _args = None
    cluster = False
    build_kwargs = {}
    svcnames = []

    docker_argv = None
    if len(sys.argv) > 1 and 'docker' in sys.argv:
        pos = sys.argv.index('docker')
        if len(sys.argv) > pos + 1:
            docker_argv = sys.argv[pos+1:]
        else:
            docker_argv = []
        sys.argv = sys.argv[:pos+1]

    options, args = parser.parse_args()
    rcColor.use_color = options.color
    node.options.format = options.format

    if _args is not None:
        args = _args

    if len(args) is 0:
        node.close()
        parser.set_usage(__usage + rcOptParser.format_desc(action=["start", "stop", "print_status"])+"\n\nOptions:\n  -h, --help       Display more actions and options\n")
        parser.error("Missing action")

    if action is None:
        action = '_'.join(args)

    if not action in rcOptParser.supported_actions():
        node.close()
        node.log.error("invalid service action: %s" % str(action))
        parser.set_usage(__usage + rcOptParser.format_desc(action=action))
        parser.error("unsupported action")

    if action in ("set", "unset") or \
       (action == "get" and not options.eval) or \
       (action == "delete" and not options.unprovision):
        build_kwargs["minimal"] = True
    else:
        build_kwargs["minimal"] = False


    if len(set(["svcnames", "status"]) & set(build_kwargs.keys())) == 0:
        if hasattr(options, "parm_svcs") and options.parm_svcs is not None:
            build_kwargs["svcnames"] = options.parm_svcs.split(',')

    if hasattr(options, "parm_status") and options.parm_status is not None:
        build_kwargs["status"] = [rcStatus.status_value(s) for s in options.parm_status.split(",")]

    if hasattr(options, "parm_primary") and options.parm_primary is not None and \
       hasattr(options, "parm_secondary") and options.parm_secondary is not None:
        node.close()
        parser.error("--onlyprimary and --onlysecondary are exclusive")

    if hasattr(options, "parm_primary") and options.parm_primary is not None:
        build_kwargs["onlyprimary"] = options.parm_primary

    if hasattr(options, "parm_secondary") and options.parm_secondary is not None:
        build_kwargs["onlysecondary"] = options.parm_secondary

    if action not in ("create", "install"):
        try:
            node.build_services(**build_kwargs)
        except ex.excError as e:
            if not action in ("pull"):
                print(e, file=sys.stderr)
                build_err = True

    if node.svcs is not None and len(node.svcs) > 0:
        svcnames = list(map(lambda x: x.svcname, node.svcs))
    elif action in ("create", "install", "pull") and hasattr(options, "parm_svcs") and options.parm_svcs is not None:
        svcnames = options.parm_svcs.split(',')

    if cmd in ('svcmgr', 'svcmgr.py') and len(svcnames) == 0:
        if not build_err:
            sys.stderr.write("""No service specified. Try:
 svcmgr -s <svcname>[,<svcname>]
 svcmgr --status <status>[,<status>]
 <svcname>
""")
        return 1

    if action == 'pull' and (node.svcs is None or len(node.svcs) == 0):
        r = node.pull_services(svcnames)
        return r

    if action in ('install', 'create'):
        try:
            node.install_service(svcnames, cf=options.param_config, template=options.param_template)
            r = 0
        except Exception as e:
            print(str(e), file=sys.stderr)
            r = 1

    if hasattr(options, "parm_rid") and options.parm_rid is not None:
        rid = options.parm_rid.split(',')
    else:
        rid = []

    if options.slave is not None:
        slave = options.slave.split(',')
    else:
        slave = None

    if options.parm_tags is not None:
        tags = options.parm_tags.replace("+", ",+").split(',')
    else:
        tags = []

    if options.parm_subsets is not None:
        subsets = options.parm_subsets.split(',')
    else:
        subsets = []

    if action in ['create', 'update', 'install']:
        if action == 'update' or (action == 'create' and options.param_config is None and options.param_template is None):
            data = getattr(svcBuilder, action)(svcnames, options.resource, interactive=options.interactive, provision=options.provision)
        else:
            data = {"rid": [], "ret": 0}
        if options.provision:
            # if the user want to provision a resource defined via configuration file edition, he
            # will set --rid <rid> or --tag or --subset to point the update command to it
            rid += data.get("rid", [])

            # force a refresh of node.svcs
            # don't push to the collector yet
            if refresh_node_svcs(svcnames, build_kwargs["minimal"]) != 0:
                build_err = True

            if len(node.svcs) == 1 and (options.param_config or options.param_template):
                node.svcs[0].setenv(options.env)
                # setenv changed the service config file
                # we need to rebuild again
                if refresh_node_svcs(svcnames, build_kwargs["minimal"]) != 0:
                    build_err = True

            if len(node.svcs) == 1 and (len(rid) > 0 or options.param_config or options.param_template):
                node.svcs[0].action("provision", rid=rid, tags=tags, subsets=subsets)
        return data["ret"]

    node.options.parallel = options.parallel
    node.options.waitlock = options.parm_waitlock

    node.set_rlimit()

    for s in node.svcs:
        s.options = options
        s.force = options.force
        s.remote = options.remote
        s.cron = options.cron
        s.options.slaves = options.slaves
        s.options.slave = slave
        s.options.master = options.master
        s.options.recover = options.recover
        s.options.discard = options.discard
        if cluster:
            s.cluster = cluster
        else:
            s.cluster = options.cluster
        s.destination_node = options.parm_destination_node
        if docker_argv is not None:
            s.docker_argv = docker_argv

    if not options.daemon and (action.startswith("stop") or action in ("shutdown", "unprovision", "switch")):
        try:
            import subprocess
            p = subprocess.Popen([sys.executable] + sys.argv + ["--daemon"], stdout=None, stderr=None, stdin=None, close_fds=True, cwd=os.sep, preexec_fn=os.setpgrp)
            p.wait()
            err = p.returncode
        except (ex.excSignal, KeyboardInterrupt) as e:
            print("the action, detached as pid %d, will continue executing" % p.pid)
            err = 1
        except Exception as e:
            print(e, file=sys.stderr)
            err = 1
    else:
        try:
            err = node.do_svcs_action(action, rid=rid, tags=tags, subsets=subsets)
        except ex.excError as e:
            print(e, file=sys.stderr)
            err = 1

    try:
        import logging
        logging.shutdown()
    except:
        pass

    return err

if __name__ == "__main__":
    r = main()
    _exit(r)

