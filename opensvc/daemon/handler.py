import utilities.converters
import core.exceptions as ex
from utilities.storage import Storage

class BaseHandler(object):
    """
    Base handler class. Defines some defaults.
    """
    path = ""
    alt_paths = []
    method = "GET"
    access = {
        "roles": ["root"],
    }
    prototype = []
    stream = False
    multiplex = "on-demand"

    def rbac(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        kwargs.update(self.access)
        kwargs["path"] = options.path
        thr.rbac_requires(**kwargs)

    def parse_options(self, data):
        def options_path(options, required=True):
            for key in ("path", "svcpath", "svcname"):
                try:
                    return options[key]
                except KeyError:
                    pass
            if required:
                raise ex.HTTP(400, "required option path is not set")
            return None

        def get_option(data, opt):
            name = opt["name"]
            fmt = opt.get("format", "string")
            required = opt.get("required", False)
            if fmt != "object_path" and required and name not in data:
                raise ex.HTTP(400, "required option %s is not set" % name)
            value = data.get(name, opt.get("default"))
            if value is None:
                value = opt.get("default")
            try:
                value = getattr(utilities.converters, "convert_"+fmt)(value)
            except AttributeError:
                pass
            except Exception as exc:
                raise ex.HTTP(400, "option %s format conversion to %s error: %s" % (name, fmt, exc))
            if fmt == "object_path":
                value = options_path(data, required=required)
            return name, value

        options = Storage()
        request_options = data.get("options", {})
        for opt in self.prototype:
            name, value = get_option(request_options, opt)
            options[name] = value
        return options
