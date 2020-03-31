"""
The node management command actions and options.
"""
from utilities.storage import Storage
from utilities.optparser import OptParser, Option
from commands.node.parser import GLOBAL_OPT, GLOBAL_OPTS

PROG = "om pool"

OPT = Storage({
    "access": Option(
        "--access", default="rwo", action="store", dest="access",
        help="The access mode of the volume. rwo, roo, rwx, rox."),
    "blk": Option(
        "--blk", default=False,
        action="store_true", dest="blk",
        help="Create a block volume instead of a formatted volume."),
    "name": Option(
        "--name", action="store", dest="name",
        help="The name of the object."),
    "namespace": Option(
        "--namespace", action="store", dest="namespace",
        help="The namespace to switch to for the action. Namespaces are cluster partitions. A default namespace can be set for the session setting the OSVC_NAMESPACE environment variable."),
    "nodes": Option(
        "--nodes", default="",
        action="store", dest="nodes",
        help="A node selector expression. Used as the created volume nodes."),
    "pool": Option(
        "--pool", default=None,
        action="store", dest="pool",
        help="The name of the storage pool."),
    "shared": Option(
        "--shared", default=False,
        action="store_true", dest="shared",
        help="Create a volume service for a shared volume resource."),
    "size": Option(
        "--size", default="rwo", action="store", dest="size",
        help="The size mode of the volume. ex: 10gi, 10g."),
    "verbose": Option(
        "--verbose", default=False,
        action="store_true", dest="verbose",
        help="Include more information to some print commands output. "
             "For example, add the ``next run`` column in the output of "
             ":cmd:`om node print schedule`."),
})
OPT.update(GLOBAL_OPT)

ACTIONS = {
    "Pool actions": {
        "ls": {
            "msg": "List the available pools.",
        },
        "status": {
            "msg": "Show pools status.",
            "options": [
                OPT.name,
                OPT.verbose,
            ],
        },
        "create_volume": {
            "msg": "Create a volume in the pool.",
            "options": [
                OPT.access,
                OPT.blk,
                OPT.name,
                OPT.namespace,
                OPT.pool,
                OPT.shared,
                OPT.size,
                OPT.nodes,
            ],
        },
    },
}

class PoolOptParser(OptParser):
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

