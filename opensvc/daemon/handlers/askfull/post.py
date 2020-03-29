import daemon.handler
import daemon.shared as shared
import core.exceptions as ex
from env import Env

class Handler(daemon.handler.BaseHandler):
    """
    Reset the generation number of the dataset of a peer node to force him
    to resend a full.
    """
    routes = (
        ("POST", "ask_full"),
        (None, "ask_full"),
    )
    prototype = [
        {
            "name": "peer",
            "format": "string",
            "desc": "The peer node to ask a full data sync to.",
            "required": True,
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.peer is None:
            raise ex.Error("The 'peer' option must be set")
        if options.peer == Env.nodename:
            raise ex.Error("Can't ask a full from ourself")
        if options.peer not in thr.cluster_nodes:
            raise ex.Error("Can't ask a full from %s: not in cluster.nodes" % options.peer)
        shared.REMOTE_GEN[options.peer] = 0
        result = {
            "info": "remote %s asked for a full" % options.peer,
            "status": 0,
        }
        return result


