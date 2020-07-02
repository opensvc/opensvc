"""
The node management command actions and options.
"""
from utilities.storage import Storage
from utilities.optparser import OptParser, Option
from commands.node.parser import GLOBAL_OPT, GLOBAL_OPTS

PROG = "om daemon"

OPT = Storage({
    "foreground": Option(
        "-f", "--foreground", default=False,
        action="store_true", dest="foreground",
        help="Run the deamon in foreground mode."),
    "id": Option(
        "--id", default=0,
        action="store", dest="id",
        help="Specify an id to act on."),
    "name": Option(
        "--name", action="store", dest="name",
        help="The name of the object."),
    "secret": Option(
        "--secret", default=None,
        action="store", dest="secret",
        help="The cluster secret used as the AES key in the cluster "
             "communications."),
    "thr_id": Option(
        "--thread-id", default=None, action="store", dest="thr_id",
        help="Specify a daemon thread, as listed in the :cmd:`om daemon "
             "status` output."),
    "timeout": Option(
        "--timeout",
        action="store", dest="time",
        help="Maximum wait time."),
})
OPT.update(GLOBAL_OPT)

ACTIONS = {
    "Daemon management actions": {
        "relay_status": {
            "msg": "Show the daemon relay clients and last update timestamp.",
        },
        "blacklist_status": {
            "msg": "Show the content of the daemon senders blacklist.",
        },
        "blacklist_clear": {
            "msg": "Empty the content of the daemon senders blacklist.",
        },
        "lock_show": {
            "msg": "Show cluster locks.",
        },
        "lock_release": {
            "msg": "Release a lock. Beware locks should be released automatically.",
            "options": [
                OPT.id,
                OPT.name,
                OPT.timeout,
            ],
        },
        "restart": {
            "msg": "Restart the daemon.",
        },
        "running": {
            "msg": "Return with code 0 if the daemon is running, else return "
                   "with code 1",
        },
        "shutdown": {
            "msg": "Stop all local services instances then stop the daemon.",
        },
        "status": {
            "msg": "Display the daemon status.",
        },
        "stats": {
            "msg": "Display the daemon stats.",
        },
        "start": {
            "msg": "Start the daemon or a daemon thread pointed by :opt:`--thread-id`.",
            "options": [
                OPT.thr_id,
                OPT.foreground,
            ],
        },
        "stop": {
            "msg": "Stop the daemon or a daemon thread pointed by :opt:`--thread-id`.",
            "options": [
                OPT.thr_id,
            ],
        },
        "join": {
            "msg": "Join the cluster of the node specified by :opt:`--node <node>`, authenticating with :opt:`--secret <secret>`.",
            "options": [
                OPT.secret,
            ],
        },
        "rejoin": {
            "msg": "Rejoin the cluster of the node specified by :opt:`--node <node>`, authenticating with the already known secret. This will re-merge the remote node cluster-wide configurations in the local node configuration file.",
        },
        "leave": {
            "msg": "Inform peer nodes we leave the cluster. Make sure the "
                   "left nodes are no longer in the services nodes list "
                   "before leaving, so the other nodes won't takeover.",
        },
        "dns_dump": {
            "msg": "Dump the content of the cluster zone.",
        },
    },
}

class DaemonOptParser(OptParser):
    """
    The daemon management command options parser class.
    """
    def __init__(self, args=None, colorize=True, width=None, formatter=None,
                 indent=6):
        OptParser.__init__(self, args=args, prog=PROG, options=OPT,
                           actions=ACTIONS,
                           global_options=GLOBAL_OPTS,
                           colorize=colorize, width=width,
                           formatter=formatter, indent=indent)

