import fnmatch

import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the cluster's storage pools information.
    """
    routes = (
        ("GET", "pools"),
        (None, "get_pools"),
    )
    prototype = [
        {
            "name": "name",
            "desc": "Limit the extract to the pools which name matches a glob pattern.",
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.name:
            pools = [name for name in shared.NODE.pool_ls_data() if fnmatch.fnmatch(name, options.name)]
        else:
            pools = None
        mon_status = thr.daemon_status_data.get(["monitor"])
        data = shared.NODE.pool_status_data(pools=pools, mon_status=mon_status)
        return data

