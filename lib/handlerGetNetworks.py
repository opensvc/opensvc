import handler
import osvcd_shared as shared

class Handler(handler.Handler):
    """
    Return the networks information.
    """
    routes = (
        ("GET", "networks"),
        (None, "get_networks"),
    )
    prototype = []
    access = {
        "roles": "guest",
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        data = shared.NODE.network_status_data()
        return data

