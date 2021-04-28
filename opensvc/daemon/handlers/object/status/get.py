import time

import daemon.handler
import daemon.shared as shared
import core.exceptions as ex
from utilities.storage import Storage
from utilities.naming import split_path, fmt_path, is_service

class Handler(daemon.handler.BaseHandler):
    """
    Get the selected object status.
    """
    routes = (
        ("GET", "object_status"),
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
        "roles": ["guest"],
        "namespaces": "FROM:path",
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        data = thr.object_data(options.path)
        return data

