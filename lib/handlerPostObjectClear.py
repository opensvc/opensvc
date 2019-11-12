import handler
import osvcd_shared as shared
from rcUtilities import split_path

class Handler(handler.Handler):
    """
    Clear the object monitor status. For example, a "start failed".
    """
    routes = (
        ("POST", "object_clear"),
        (None, "clear"),
    )
    prototype = [
        {
            "name": "path",
            "desc": "The object path.",
            "required": True,
            "format": "object_path",
        },
    ]
    access = {
        "roles": ["admin"],
        "namespaces": "FROM:path",
    }

    def rbac(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        _, namespace, _ = split_path(options.path)
        thr.rbac_requires(roles=["admin"], namespaces=[namespace], **kwargs)

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        smon = thr.get_service_monitor(options.path)
        if smon.status.endswith("ing"):
            return {"info": "skip clear on %s instance" % smon.status, "status": 0}
        thr.log_request("service %s clear" % options.path, nodename, **kwargs)
        thr.set_smon(options.path, status="idle", reset_retries=True)
        return {"status": 0, "info": "%s instance cleared" % options.path}

