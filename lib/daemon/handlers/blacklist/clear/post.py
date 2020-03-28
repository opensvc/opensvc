import daemon.handler

class Handler(daemon.handler.BaseHandler):
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

