from __future__ import print_function

import os
import sys

import utilities.render.color
import core.exceptions as ex
from commands.pool.parser import PoolOptParser
from core.node import Node


def _main(node, argv=None):
    optparser = PoolOptParser(argv)
    options, action = optparser.parse_args(argv)

    utilities.render.color.use_color = options.color
    node.options.update(options.__dict__)

    node.check_privs(action)

    err = 0
    try:
        err = node.action("pool_"+action)
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

