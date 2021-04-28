import daemon.handler
import daemon.shared as shared
import core.exceptions as ex

class Handler(daemon.handler.BaseHandler):
    """
    Set or unset properties of a node monitor.
    """
    routes = (
        ("POST", "node_monitor"),
        (None, "set_node_monitor"),
    )
    prototype = [
        {
            "name": "local_expect",
            "desc": "The expected state on node.",
            "required": False,
            "format": "string",
        },
        {
            "name": "global_expect",
            "desc": "The expected state clusterwide.",
            "required": False,
            "format": "string",
            "candidates": [
                "thawed",
                "frozen",
                "unset",
            ],
        },
        {
            "name": "status",
            "desc": "The state on node.",
            "required": False,
            "format": "string",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        info = []
        error = []
        data = {"data": {}}
        try:
            self.validate_cluster_global_expect(options.global_expect, thr=thr)
        except ex.AbortAction as exc:
            info.append(str(exc))
        except ex.Error as exc:
            error.append(str(exc))
        else:
            thr.set_nmon(
                status=options.status,
                local_expect=options.local_expect,
                global_expect=options.global_expect,
            )
            if options.global_expect:
                data["data"]["global_expect"] = options.global_expect
            info.append("cluster target state set to %s" % options.global_expect)
        data["status"] = len(error)
        if info:
            data["info"] = info
        if error:
            data["error"] = error
        return data

    def validate_cluster_global_expect(self, global_expect, thr=None):
        if global_expect is None:
            return
        try:
            frozen = thr.daemon_status_data.get(["monitor", "frozen"])
        except KeyError:
            frozen = None
        if global_expect == "thawed":
            if frozen == "thawed":
                raise ex.AbortAction("cluster is already thawed")
        elif global_expect == "frozen":
            if frozen == "frozen":
                raise ex.AbortAction("cluster is already frozen")
        else:
            raise ex.Error("invalid global_expect value: %s" % global_expect)

