import sys
import os
import optparse

#
# add project lib to path
#
prog = "nodemgr"

import rcStatus
import rcColor
import rcExceptions as ex
from rcGlobalEnv import *
from rcUtilities import check_privs, ximport

node_mod = ximport('node')

try:
    from version import version
except:
    version = "dev"

n = node_mod.Node()

__ver = prog + " version " + version
__usage = prog + " [ OPTIONS ] COMMAND\n\n"
parser = optparse.OptionParser(version=__ver, usage=__usage + n.format_desc())
parser.add_option("--refresh-api", default=False, action="store_true", dest="refresh_api",
                  help="The OpenSVC collector api url")
parser.add_option("--color", default="auto", action="store", dest="color",
                  help="colorize output. possible values are : auto=guess based on tty presence, always=always colorize, never=never colorize")
parser.add_option("--verbose", default=False, action="store_true", dest="verbose",
                  help="add more information to some print commands: +next in 'print schedule'")
parser.add_option("--debug", default=False,
		  action="store_true", dest="debug",
                  help="debug mode")
parser.add_option("--stats-dir", default=None,
		  action="store", dest="stats_dir",
                  help="points the directory where the metrics files are stored for pushstats")
parser.add_option("--module", default="",
		  action="store", dest="module",
                  help="compliance, set module list")
parser.add_option("--moduleset", default="",
		  action="store", dest="moduleset",
                  help="compliance, set moduleset list. The 'all' value can be used in conjonction with detach.")
parser.add_option("--ruleset", default="",
		  action="store", dest="ruleset",
                  help="compliance, set ruleset list. The 'all' value can be used in conjonction with detach.")
parser.add_option("--filterset", default="",
		  action="store", dest="filterset",
                  help="set a filterset to limit collector extractions")
parser.add_option("--ruleset-date", default="",
		  action="store", dest="ruleset_date",
                  help="compliance, use rulesets valid on specified date")
parser.add_option("--attach", default=False,
		  action="store_true", dest="attach",
                  help="attach the modulesets specified during a compliance check/fix/fixable command")
parser.add_option("--cron", default=False,
		  action="store_true", dest="cron",
                  help="cron mode")
parser.add_option("--force", default=False,
		  action="store_true", dest="force",
                  help="force action")
parser.add_option("--symcli-db-file", default=None,
		  action="store", dest="symcli_db_file",
                  help="[pushsym option] use symcli offline mode with the specified file. aclx files are expected to be found in the same directory and named either <symid>.aclx or <same_prefix_as_bin_file>.aclx")
parser.add_option("--param", default=None,
		  action="store", dest="param",
                  help="point a node configuration parameter for the 'get' and 'set' actions")
parser.add_option("--value", default=None,
		  action="store", dest="value",
                  help="set a node configuration parameter value for the 'set --param' action")
parser.add_option("--duration", default=None, action="store", dest="duration", type="int",
                  help="a duration expressed in minutes. used with the 'collector ack action' action")
parser.add_option("--begin", default=None, action="store", dest="begin",
                  help="a begin date expressed as 'YYYY-MM-DD hh:mm'. used with the 'collector ack action' and pushstats action")
parser.add_option("--end", default=None, action="store", dest="end",
                  help="a end date expressed as 'YYYY-MM-DD hh:mm'. used with the 'collector ack action' and pushstats action")
parser.add_option("--comment", default=None, action="store", dest="comment",
                  help="a comment to log when used with the 'collector ack action' action")
parser.add_option("--author", default=None, action="store", dest="author",
                  help="the acker name to log when used with the 'collector ack action' action")
parser.add_option("--id", default=0, action="store", dest="id", type="int",
                  help="specify an id to act on")
parser.add_option("--resource", default=[], action="append",
                  help="a resource definition in json dictionary format fed to the provision action")
parser.add_option("--object", default=[], action="append", dest="objects",
                  help="an object to limit a push* action to. multiple --object <object id> parameters can be set on a single command line")
parser.add_option("--mac", default=None,
		  action="store", dest="mac",
                  help="list of mac addresses, comma separated, used by the 'wol' action")
parser.add_option("--tag", default=None,
		  action="store", dest="tag",
                  help="a tag specifier used by 'collector create tag', 'collector add tag', 'collector del tag'")
parser.add_option("--like", default="%",
		  action="store", dest="like",
                  help="a sql like filtering expression. leading and trailing wildcards are automatically set.")
parser.add_option("--broadcast", default=None,
		  action="store", dest="broadcast",
                  help="list of broadcast addresses, comma separated, used by the 'wol' action")
parser.add_option("--sync", default=False, action="store_true", dest="syncrpc",
                  help="use synchronous collector rpc if available. to use with pushasset when chaining a compliance run, to make sure the node ruleset is up-to-date.")
parser.add_option("--user", default=None, action="store", dest="user",
                  help="authenticate with the collector using the specified user credentials instead of the node credentials. Required for the 'register' action when the collector is configured to refuse anonymous register.")
parser.add_option("--password", default=None, action="store", dest="password",
                  help="authenticate with the collector using the specified user credentials instead of the node credentials. Prompted if necessary but not specified.")
parser.add_option("--insecure", default=False, action="store_true", dest="insecure",
                  help="allow communications with a collector presenting unverified SSL certificates.")
parser.add_option("--api", default=None, action="store", dest="api",
                  help="specify a collector api url different from the one set in node.conf. Honored by the 'collector cli' action.")
parser.add_option("--config", default=None, action="store", dest="config",
                  help="specify a user-specific collector api connection configuration file. defaults to '~/.opensvc-cli'. Honored by the 'collector cli' action.")
parser.add_option("--app", default=None, action="store", dest="app",
                  help="Optional with the register command, register the node in the specified app. If not specified, the node is registered in the first registering user's app found.")
parser.add_option("--format", default=None, action="store", dest="format",
                  help="specify a data formatter for output of the print* and collector* commands. possible values are json or table.")


(options, args) = parser.parse_args()
rcColor.use_color = options.color

n.options = options

def do_symcli_db_file(symcli_db_file):
    if symcli_db_file is None:
        return
    if not os.path.exists(symcli_db_file):
        print("File does not exist: %s"%symcli_db_file)
        return
    os.environ['SYMCLI_DB_FILE'] = symcli_db_file
    os.environ['SYMCLI_OFFLINE'] = '1'

def _exit(r):
    n.close()
    sys.exit(r)

def main():
    do_symcli_db_file(options.symcli_db_file)

    if len(args) is 0:
        n.close()
        parser.error("Missing action")
    action = '_'.join(args)
    if action.startswith("collector_cli"):
        action = "collector_cli"
    if not action in n.supported_actions():
        n.close()
        parser.set_usage(__usage + n.format_desc(action))
        parser.error("unsupported action: %s"%action)

    if action not in n.unprivileged_actions:
        check_privs()

    err = 0
    try:
        err = n.action(action)
    except KeyboardInterrupt:
        sys.stderr.write("Keybord Interrupt\n")
        err = 1
    except ex.excError:
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        sys.stderr.write(str(exc_value)+'\n')
        err = 1
    except:
        raise
        err = 1
    return err

if __name__ == "__main__":
    r = main()
    _exit(r)
