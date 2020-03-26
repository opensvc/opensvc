import traceback

import daemon.handlers.handler as handler
import daemon.shared as shared
import core.exceptions as ex
from rcUtilities import split_path

class Handler(handler.Handler):
    """
    Return the list of keys of a usr, cfg or sec object key.
    """
    routes = (
        ("GET", "object_keys"),
    )
    access = {
        "roles": ["guest"],
        "namespaces": "FROM:path",
    }
    prototype = [
        {
            "name": "path",
            "desc": "The object path.",
            "required": True,
            "format": "object_path",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        try:
            return {"status": 0, "data": shared.SERVICES[options.path].data_keys()}
        except ex.Error as exc:
            return {"status": 1, "error": str(exc)}
        except Exception as exc:
            return {"status": 1, "error": str(exc), "traceback": traceback.format_exc()}

