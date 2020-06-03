from __future__ import print_function
from __future__ import absolute_import

import os
import sys

import utilities.render.color
import core.exceptions as ex
from env import Env
from commands.node.parser import NodeOptParser
from core.node import Node
from utilities.proc import get_extra_argv


def do_symcli_db_file(options):
    try:
        symcli_db_file = options.symcli_db_file
    except AttributeError:
        return
    if symcli_db_file is None:
        return
    if not os.path.exists(symcli_db_file):
        print("File does not exist: %s" % symcli_db_file)
        return
    os.environ['SYMCLI_DB_FILE'] = symcli_db_file
    os.environ['SYMCLI_OFFLINE'] = '1'

def _main(node, argv=None):
    argv, extra_argv = get_extra_argv(argv)
    optparser = NodeOptParser(argv)
    options, action = optparser.parse_args(argv)
    options.extra_argv = extra_argv

    utilities.render.color.use_color = options.color
    node.options.update(options.__dict__)
    do_symcli_db_file(options)

    if action.startswith("collector_cli"):
        action = "collector_cli"

    node.check_privs(action)

    err = 0
    try:
        err = node.action(action)
    except KeyboardInterrupt:
        sys.stderr.write("Keybord Interrupt\n")
        err = 1
    except ex.Error:
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

def main(argv=None):
    node = Node()

    try:
        return _main(node, argv=argv)
    except ex.Error as exc:
        print(exc, file=sys.stderr)
        return 1
    except ex.Version as exc:
        print(exc)
        return 0
    finally:
        node.close()

if __name__ == "__main__":
    ret = main()
    sys.exit(ret)

