import daemon.handler

class Handler(daemon.handler.BaseHandler):
    """
    Clear the object monitor status. For example, a "start failed".
    Transient status are not clearable (those ending with 'ing', like starting).
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
        "roles": ["operator"],
        "namespaces": "FROM:path",
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        smon = thr.get_service_monitor(options.path)
        if not smon.status:
            return {"info": "skip clear on instance (no monitor data)" % smon.status, "status": 0}
        if smon.status.endswith("ing"):
            return {"info": "skip clear on %s instance" % smon.status, "status": 0}
        thr.log_request("clear %s monitor status" % options.path, nodename, **kwargs)
        thr.set_smon(options.path, status="idle", reset_retries=True)
        return {"status": 0, "info": "%s instance cleared" % options.path}

