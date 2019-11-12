import handler
import osvcd_shared as shared
from rcUtilities import split_path

class Handler(handler.Handler):
    """
    Clear the senders blacklist.
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
            shared.wake_monitor(reason="service %s notification" % options.path)
        else:
            shared.wake_monitor(reason="node notification")
        return {"status": 0}

