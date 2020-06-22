import daemon.handler
import daemon.shared as shared
import core.exceptions as ex
from utilities.naming import factory

class Handler(daemon.handler.BaseHandler):
    """
    Return the supported <kind> object keywords.
    """
    routes = (
        ("GET", "keywords"),
        (None, "get_keywords"),
    )
    prototype = [
        {
            "name": "kind",
            "format": "string",
            "required": True,
            "candidates": ["node", "svc", "vol", "usr", "sec", "cfg", "ccfg"],
            "desc": "The object kind or 'node'.",
        },
    ]
    access = {}

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.kind == "node":
            obj = shared.NODE
        elif options.kind:
            obj = factory(options.kind)(name="dummy", node=shared.NODE, volatile=True)
        else:
            raise ex.HTTP(400, "A kind must be specified.")
        return obj.full_kwstore.dump()

