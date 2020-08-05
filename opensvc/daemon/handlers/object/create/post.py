import json
from subprocess import PIPE

import daemon.handler
import daemon.rbac
import core.exceptions as ex
from utilities.naming import validate_paths
from utilities.string import bdecode

class Handler(daemon.handler.BaseHandler, daemon.rbac.ObjectCreateMixin):
    """
    Create new objects.
    """
    routes = (
        ("POST", "object_create"),
        ("POST", "create"),
        (None, "create"),
    )
    prototype = [
        {
            "name": "path",
            "desc": "The path of the object to execute the action on.",
            "required": False,
            "format": "object_path",
        },
        {
            "name": "template",
            "desc": "The name of a template to get the configuration from.",
            "required": False,
            "format": "string",
        },
        {
            "name": "namespace",
            "desc": "The namespace to create the objects in, overriding the namespace part of object paths.",
            "required": False,
            "format": "string",
        },
        {
            "name": "data",
            "desc": "The dictionnary of object configurations, indexed by object path. If template is set, the data option can be used to pass env section overrides. Examples: {'k': 'v'} or {'env': {'k': 'v'}} or {'ns1/svc/svc1': {'k': 'v'}} or {'ns1/svc/svc1': {'env': {'k': 'v'}}}",
            "required": False,
            "format": "dict",
            "default": {},
        },
        {
            "name": "provision",
            "desc": "If true, the object is marked for provisioning upon create.",
            "required": False,
            "format": "boolean",
            "default": False,
        },
        {
            "name": "restore",
            "desc": "If true, the object id provided in the configuration data is preserve. The default is to generate a new object id upon create.",
            "required": False,
            "format": "boolean",
            "default": False,
        },
        {
            "name": "sync",
            "desc": "If true, the object is created synchronously.",
            "required": False,
            "format": "boolean",
            "default": True,
        },
    ]
    access = "custom"

    def rbac(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.template is not None:
            thr.rbac_requires(roles=["admin"], namespaces=[options.namespace], **kwargs)
            return
        if not options.data:
            return
        errors = self.rbac_create_data(payload=options.data, thr=thr, **kwargs)
        if errors:
            raise ex.HTTP(403, errors)

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if not options.data and not options.template:
            return {"status": 0, "info": "no data"}
        if options.template is not None:
            if options.path:
                paths = [options.path]
                cmd = ["create", "-s", options.path, "--template=%s" % options.template, "--env=-"]
            else:
                paths = [p for p in options.data]
                cmd = ["create", "--template=%s" % options.template, "--env=-"]
        else:
            paths = [p for p in options.data]
            cmd = ["create", "--config=-"]
        validate_paths(paths)
        if options.namespace:
            cmd.append("--namespace="+options.namespace)
        if options.restore:
            cmd.append("--restore")
        thr.log_request("create/update %s" % ",".join(paths), nodename, **kwargs)
        proc = thr.service_command(None, cmd, stdout=PIPE, stderr=PIPE, stdin=json.dumps(options.data))
        if options.sync:
            out, err = proc.communicate()
            result = {
                "status": proc.returncode,
                "data": {
                    "out": bdecode(out),
                    "err": bdecode(err),
                    "ret": proc.returncode,
                },
            }
        else:
            thr.parent.push_proc(proc)
            result = {
                "status": 0,
                "info": "started %s action %s" % (options.path, " ".join(cmd)),
            }
        if options.provision:
            for path in paths:
                thr.set_smon(path, global_expect="provisioned")
        return result

