"""
The node management command actions and options.
"""
from utilities.storage import Storage
from utilities.optparser import OptParser, Option
from commands.node.parser import GLOBAL_OPT, GLOBAL_OPTS

PROG = "om network"

OPT = Storage({
    "name": Option(
        "--name", action="store", dest="name",
        help="The name of the object."),
    "verbose": Option(
        "--verbose", default=False,
        action="store_true", dest="verbose",
        help="Include more information to some print commands output. "
             "For example, add the ``next run`` column in the output of "
             ":cmd:`om node print schedule`."),
})
OPT.update(GLOBAL_OPT)

ACTIONS = {
    "Network actions": {
        "ls": {
            "msg": "List the available networks.",
        },
        "setup": {
            "msg": "Create bridges, assign host address, update host routes to node backend networks. This action is executed on node configuration changes. Useful for troubleshoot.",
        },
        "show": {
            "msg": "Show network configuration.",
            "options": [
                OPT.name,
            ],
        },
        "status": {
            "msg": "Show allocated ip address in networks.",
            "options": [
                OPT.name,
                OPT.verbose,
            ],
        },
    },
}

class NetworkOptParser(OptParser):
    """
    The pool management command options parser class.
    """
    def __init__(self, args=None, colorize=True, width=None, formatter=None,
                 indent=6):
        OptParser.__init__(self, args=args, prog=PROG, options=OPT,
                           actions=ACTIONS,
                           global_options=GLOBAL_OPTS,
                           colorize=colorize, width=width,
                           formatter=formatter, indent=indent)

