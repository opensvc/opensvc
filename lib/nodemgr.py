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
import nodemgr_parser

node_mod = ximport('node')

try:
    from version import version
except:
    version = "dev"

def do_symcli_db_file(symcli_db_file):
    if symcli_db_file is None:
        return
    if not os.path.exists(symcli_db_file):
        print("File does not exist: %s"%symcli_db_file)
        return
    os.environ['SYMCLI_DB_FILE'] = symcli_db_file
    os.environ['SYMCLI_OFFLINE'] = '1'

def main(node):
    optparser = nodemgr_parser.OptParser()
    options, args = optparser.parser.parse_args()
    action = optparser.get_action_from_args(args, options)

    rcColor.use_color = options.color
    node.options = options
    do_symcli_db_file(options.symcli_db_file)

    if action.startswith("collector_cli"):
        action = "collector_cli"

    if action not in node.unprivileged_actions:
        check_privs()

    err = 0
    try:
        err = node.action(action)
    except KeyboardInterrupt:
        sys.stderr.write("Keybord Interrupt\n")
        err = 1
    except ex.excError:
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        es = str(exc_value)
        if len(es) > 0:
            sys.stderr.write(str(exc_value)+'\n')
        err = 1
    except:
        raise
        err = 1
    return err

if __name__ == "__main__":
    node = node_mod.Node()

    try:
        r = main(node)
    finally:
        node.close()
    sys.exit(r)
