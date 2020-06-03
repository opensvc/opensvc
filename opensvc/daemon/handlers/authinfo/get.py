import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the supported authentication methods.
    """
    routes = (
        ("GET", "authinfo"),
        (None, "authinfo"),
    )
    prototype = []
    access = {}
    multiplex = "never"

    def action(self, nodename, thr=None, **kwargs):
        data = {
            "methods": ["basic"],
        }
        well_known_uri = shared.NODE.oget("listener", "openid_well_known")
        if well_known_uri:
            data["methods"].append("openid")
            data["openid"] = {
                "well_known_uri": well_known_uri,
                "client_id": thr.cluster_name,
            }
        return data

