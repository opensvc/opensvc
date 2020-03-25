import daemon.handlers.handler as handler
import daemon.shared as shared

class Handler(handler.Handler):
    """
    Clear the senders blacklist.
    """
    routes = (
        ("POST", "blacklist_clear"),
    )
    prototype = []
    access = {
        "roles": ["blacklistadmin"],
    }

    def action(self, nodename, thr=None, **kwargs):
        thr.blacklist_clear()
        return {"status": 0}

