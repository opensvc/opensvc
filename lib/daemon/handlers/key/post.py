import traceback

import daemon.handler
import daemon.shared as shared
import core.exceptions as ex

class Handler(daemon.handler.BaseHandler):
    """
    Add or update the value of a usr, cfg or sec object key.
    """
    routes = (
        ("POST", "key"),
        (None, "set_key"),
    )
    access = {
        "roles": ["admin"],
        "namespaces": "FROM:path",
    }
    prototype = [
        {
            "name": "path",
            "desc": "The object path.",
            "required": True,
            "format": "object_path",
        },
        {
            "name": "key",
            "desc": "The name of the key to set a value for.",
            "required": True,
            "format": "string",
        },
        {
            "name": "data",
            "desc": "The key value to assign.",
            "required": True,
            "format": "string",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        try:
            shared.SERVICES[options.path].add_key(options.key, options.data)
            return {"status": 0, "info": "key %s value set" % options.key}
        except ex.Error as exc:
            return {"status": 1, "error": str(exc)}
        except Exception as exc:
            return {"status": 1, "error": str(exc), "traceback": traceback.format_exc()}


