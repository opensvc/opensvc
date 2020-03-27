import daemon.handler
import daemon.shared as shared
import core.exceptions as ex

class Handler(daemon.handler.BaseHandler):
    """
    Return the list templates in the trusted <catalog>.
    """
    routes = (
        ("GET", "templates"),
        (None, "get_templates"),
    )
    prototype = [
        {
            "name": "catalog",
            "required": True,
            "format": "string",
            "desc": "The name of the catalog from which to report the templates list.",
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        data = {}
        if options.catalog == "collector":
            if shared.NODE.collector_env.dbopensvc is None:
                raise ex.HTTP(400, "This node is not registered on a collector")
            data = []
            options = {
                "limit": 0,
                "props": "id,tpl_name,tpl_author,tpl_comment",
                "orderby": "tpl_name",
            }

            for tpl in shared.NODE.collector_rest_get("/provisioning_templates", options)["data"]:
                data.append({
                    "id": tpl["id"],
                    "name": tpl["tpl_name"],
                    "desc": tpl["tpl_comment"],
                    "author": tpl["tpl_author"],
                    "catalog": "collector",
                })
        else:
            raise ex.HTTP(400, "unknown catalog %s" % options.catalog)
        return data

