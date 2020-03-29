import traceback

import daemon.handler
import daemon.shared as shared
import core.exceptions as ex
from utilities.naming import split_path
from utilities.string import bdecode

class Handler(daemon.handler.BaseHandler):
    """
    Return the value of a usr, cfg or sec object key.
    """
    routes = (
        ("GET", "key"),
        (None, "get_key"),
        (None, "get_secret_key"),
    )
    access = "custom",
    prototype = [
        {
            "name": "path",
            "desc": "The object path.",
            "required": True,
            "format": "object_path",
        },
        {
            "name": "key",
            "desc": "The key name to provide value of.",
            "required": True,
            "format": "string",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        try:
            return {"status": 0, "data": bdecode(shared.SERVICES[options.path].decode_key(options.key))}
        except ex.Error as exc:
            return {"status": 1, "error": str(exc)}
        except Exception as exc:
            return {"status": 1, "error": str(exc), "traceback": traceback.format_exc()}

    def rbac(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        name, namespace, kind = split_path(options.path)
        if kind == "cfg":
            role = "guest"
        else:
            # sec, usr
            role = "admin"
        thr.rbac_requires(roles=[role], namespaces=[namespace], **kwargs)

