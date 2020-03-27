import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the list of catalogs trusted by the cluster.
    """
    routes = (
        ("GET", "catalogs"),
        (None, "get_catalogs"),
    )
    prototype = []
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        data = []
        if shared.NODE.collector_env.dbopensvc is not None:
            data.append({
                "name": "collector",
            })
        return data

