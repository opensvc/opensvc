import daemon.handler

from utilities.naming import split_path, factory

class Handler(daemon.handler.BaseHandler):
    """
    Return the list of the object resource identifiers that require a
    run confirmation.
    Used by the webapp to ask for confirmation when the user submits a
    run action.
    """
    routes = (
        ("GET", "object_confirmations"),
    )
    prototype = [
        {
            "name": "path",
            "desc": "The object to return task confirmations of.",
            "required": True,
            "format": "string",
        },
    ]
    access = {
        "roles": ["operator"],
        "namespaces": "FROM:path",
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        name, namespace, kind = split_path(options.path)
        svc = factory(kind)(name=name, namespace=namespace, volatile=True)
        data = {
            "status": 0,
            "data": [res.rid for res in svc.get_resources("task") if res.confirmation],
        }
        return data

