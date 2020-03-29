import daemon.handler
import daemon.shared as shared
from utilities.naming import split_path

class Handler(daemon.handler.BaseHandler):
    """
    Wake the monitor thread loop as soon as possible.
    Used by the CRM commands to signal an instance status change is ready to be processed by the daemon.
    """
    routes = (
        ("POST", "wake_monitor"),
        (None, "wake_monitor"),
    )
    prototype = [
        {
            "name": "path",
            "desc": "The object path.",
            "required": False,
            "format": "object_path",
        },
        {
            "name": "reason",
            "desc": "The reason why the caller wants the monitor thread woken.",
            "required": False,
        },
        {
            "name": "immediate",
            "desc": "Should the monitor thread be woken immediately. If False, the monitor thread will be woken on next short-loop.",
            "required": False,
            "default": False,
            "format": "boolean",
        },
    ]
    access = "custom"

    def rbac(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.path:
            name, namespace, kind = split_path(options.path)
            thr.rbac_requires(roles=["operator"], namespaces=[namespace], **kwargs)
        else:
            thr.rbac_requires(roles=["operator"], namespaces="ANY", **kwargs)

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.path:
            reason = options.reason or "service %s notification" % options.path
            shared.wake_monitor(reason=reason, immediate=options.immediate)
        else:
            reason = options.reason or "node notification"
            shared.wake_monitor(reason=reason, immediate=options.immediate)
        return {"status": 0}

