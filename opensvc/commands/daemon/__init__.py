from __future__ import print_function

import sys

import core.exceptions as ex
import utilities.render.color
from commands.daemon.parser import DaemonOptParser
from core.node import Node


def _main(node, argv=None):
    optparser = DaemonOptParser(argv)
    options, action = optparser.parse_args(argv)

    utilities.render.color.use_color = options.color
    node.options.update(options.__dict__)

    node.check_privs(action)

    try:
        return node.action("daemon_"+action)
    except KeyboardInterrupt:
        sys.stderr.write("Keyboard Interrupt\n")
        return 1
    except ex.Error:
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        es = str(exc_value)
        if len(es) > 0:
            sys.stderr.write(str(exc_value)+'\n')
        return 1
    except:
        raise


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
