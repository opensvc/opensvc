import daemon.handlers.handler as handler
import daemon.shared as shared

class Handler(handler.Handler):
    """
    Return the senders blacklist.
    """
    routes = (
        ("GET", "blacklist_status"),
        ("GET", "daemon_blacklist_status"),
        (None, "daemon_blacklist_status"),
    )
    prototype = []
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        return {"status": 0, "data": thr.get_blacklist()}

