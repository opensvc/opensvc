import daemon.handler
import daemon.shared as shared
import core.exceptions as ex

class Handler(daemon.handler.BaseHandler):
    """
    Return the <template> data from the trusted <catalog>.
    """
    routes = (
        ("GET", "template"),
        (None, "get_template"),
    )
    prototype = [
        {
            "name": "catalog",
            "required": True,
            "format": "string",
            "desc": "The name of the catalog hosting the template.",
        },
        {
            "name": "template",
            "required": True,
            "format": "string",
            "desc": "The name or id of the template in the catalog.",
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.catalog == "collector":
            if options.template is None:
                raise ex.HTTP(400, "template is not set")
            request_options = {
                "props": "tpl_definition"
            }
            try:
                data = shared.NODE.collector_rest_get("/provisioning_templates/%s" % options.template, request_options)
                return data["data"][0]["tpl_definition"]
            except IndexError:
                raise ex.HTTP(404, "template not found")
        raise ex.HTTP(400, "unknown catalog %s" % options.catalog)

