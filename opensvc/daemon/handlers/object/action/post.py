import importlib
import json
import os
from subprocess import Popen, PIPE

import daemon.handler
import daemon.rbac
import daemon.shared as shared
import core.exceptions as ex
from utilities.naming import split_path
from env import Env
from utilities.string import bdecode

GUEST_ACTIONS = (
    "eval",
    "get",
    "keys",
    "print_config_mtime",
)

OPERATOR_ACTIONS = (
    "clear",
    "disable",
    "enable",
    "freeze",
    "push_status",
    "push_resinfo",
    "push_config",
    "push_encap_config",
    "presync",
    "prstatus",
    "resource_monitor",
    "restart",
    "resync",
    "run",
    "scale",
    "snooze",
    "start",
    "startstandby",
    "status",
    "stop",
    "stopstandby",
    "thaw",
    "unsnooze",
)

ADMIN_ACTIONS = (
    "add",
    "boot",
    "decode",
    "delete",
    "gen_cert",
    "install",
    "pg_kill",
    "pg_freeze",
    "pg_thaw",
    "provision",
    "run",
    "set_provisioned",
    "set_unprovisioned",
    "shutdown",
    "unprovision",
    "unset",
)

class Handler(daemon.handler.BaseHandler, daemon.rbac.ObjectCreateMixin):
    """
    Execute an object instance action.
    """
    routes = (
        ("POST", "object_action"),
        ("POST", "service_action"),
        (None, "service_action"),
    )
    prototype = [
        {
            "name": "path",
            "desc": "The path of the object to execute the action on.",
            "required": True,
            "format": "object_path",
        },
        {
            "name": "sync",
            "desc": "Execute synchronously and return the outputs.",
            "required": False,
            "default": True,
            "format": "boolean",
        },
        {
            "name": "cmd",
            "desc": "The command vector.",
            "required": False,
            "format": "list",
            "deprecated": True,
        },
        {
            "name": "action",
            "desc": "The action to execute.",
            "required": False,
            "format": "string",
        },
        {
            "name": "options",
            "desc": "The action options.",
            "required": False,
            "format": "dict",
            "default": {},
        },
    ]
    access = "custom"

    def rbac(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        name, namespace, kind = split_path(options.path)

        if options.action in GUEST_ACTIONS:
            role = "guest"
        elif options.action in OPERATOR_ACTIONS:
            role = "operator"
        elif options.action in ADMIN_ACTIONS:
            role = "admin"
        else:
            role = "root"

        if options.action == "set":
            # load current config
            try:
                cf = shared.SERVICES[options.path].print_config_data()
            except Exception as exc:
                cf = {}

            # purge unwanted sections
            try:
                del cf["metadata"]
            except Exception:
                pass

            # merge changes in current config
            for buff in options.options.get("kw", []):
                k, v = buff.split("=", 1)
                if k[-1] in ("+", "-"):
                    k = k[:-1]
                k = k.strip()
                try:
                    s, k = k.split(".", 1)
                except Exception:
                    s = "DEFAULT"
                if s not in cf:
                    cf[s] = {}
                cf[s][k] = v

            # apply object create rbac to the amended config
            payload = {options.path: cf}
            errors = self.rbac_create_data(payload=payload, thr=thr, **kwargs)
            if errors:
                raise ex.HTTP(403, errors)
        else:
            thr.rbac_requires(roles=[role], namespaces=[namespace], **kwargs)

        if options.cmd:
            # compat, requires root
            kwargs["roles"] = ["root"]
            thr.rbac_requires(**kwargs)


    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        name, namespace, kind = split_path(options.path)

        if thr.get_service(options.path) is None and options.action not in ("create", "deploy"):
            thr.log_request("service action (%s not installed)" % options.path, nodename, lvl="warning", **kwargs)
            raise ex.HTTP(404, "%s not found" % options.path)
        if not options.action and not options.cmd:
            thr.log_request("service action (no action set)", nodename, lvl="error", **kwargs)
            raise ex.HTTP(400, "action not set")

        for opt in ("node", "daemon", "svcs", "service", "s", "parm_svcs", "local", "id"):
            if opt in options.options:
                del options.options[opt]
        for opt, ropt in (("jsonpath_filter", "filter"),):
            if opt in options.options:
                options.options[ropt] = options.options[opt]
                del options.options[opt]
        options.options["local"] = True
        pmod = importlib.import_module("commands.{kind}mgr.parser".format(kind=kind))
        popt = pmod.OPT

        def find_opt(opt):
            for k, o in popt.items():
                if o.dest == opt:
                    return o
                if o.dest == "parm_" + opt:
                    return o

        if options.cmd:
            cmd = [options.cmd]
        else:
            cmd = [options.action]
            for opt, val in options.options.items():
                po = find_opt(opt)
                if po is None:
                    continue
                if val == po.default:
                    continue
                if val is None:
                    continue
                opt = po._long_opts[0] if po._long_opts else po._short_opts[0]
                if po.action == "append":
                    cmd += [opt + "=" + str(v) for v in val]
                elif po.action == "store_true" and val:
                    cmd.append(opt)
                elif po.action == "store_false" and not val:
                    cmd.append(opt)
                elif po.type == "string":
                    opt += "=" + val
                    cmd.append(opt)
                elif po.type == "integer":
                    opt += "=" + str(val)
                    cmd.append(opt)

        fullcmd = Env.python_cmd + ["-m", Env.package, options.path] + cmd
        thr.log_request("run 'om %s %s'" % (options.path, " ".join(cmd)), nodename, **kwargs)
        if options.sync:
            proc = Popen(fullcmd, stdout=PIPE, stderr=PIPE, stdin=None, close_fds=True)
            out, err = proc.communicate()
            try:
                result = json.loads(out)
            except Exception:
                result = {
                    "status": 0,
                    "data": {
                        "out": bdecode(out),
                        "err": bdecode(err),
                        "ret": proc.returncode,
                    },
                }
        else:
            proc = Popen(fullcmd, stdin=None, close_fds=True)
            thr.push_proc(proc)
            result = {
                "status": 0,
                "info": "started %s action %s" % (options.path, " ".join(cmd)),
            }
        return result

