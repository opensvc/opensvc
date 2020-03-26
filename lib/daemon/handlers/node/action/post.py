import os
from subprocess import Popen, PIPE

import daemon.handlers.handler as handler
import daemon.shared as shared
import core.exceptions as ex
from rcUtilities import drop_option
from rcGlobalEnv import rcEnv
from utilities.string import bdecode

class Handler(handler.Handler):
    """
    Execute a node action.
    """
    routes = (
        ("POST", "node_action"),
        (None, "node_action"),
    )
    prototype = [
        {
            "name": "sync",
            "desc": "Execute synchronously and return the outputs.",
            "required": False,
            "default": True,
            "format": "boolean",
        },
        {
            "name": "action_mode",
            "desc": "If true, adds --local if not already present in <cmd> or <options>.",
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

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)

        if not options.cmd and not options.action:
            thr.log_request("node action ('action' not set)", nodename, lvl="error", **kwargs)
            return {
                "status": 1,
            }

        for opt in ("node", "server", "daemon"):
            if opt in options.options:
                del options.options[opt]
        if options.action_mode and options.options.get("local"):
            if "local" in options.options:
                del options.options["local"]
        for opt, ropt in (("jsonpath_filter", "filter"),):
            if opt in options.options:
                options.options[ropt] = options.options[opt]
                del options.options[opt]
        options.options["local"] = True
        pmod = __import__("nodemgr_parser")
        popt = pmod.OPT

        def find_opt(opt):
            for k, o in popt.items():
                if o.dest == opt:
                    return o
                if o.dest == "parm_" + opt:
                    return o

        if options.cmd:
            cmd = [] + options.cmd
            cmd = drop_option("--node", cmd, drop_value=True)
            cmd = drop_option("--server", cmd, drop_value=True)
            cmd = drop_option("--daemon", cmd)
            if options.action_mode and "--local" not in cmd:
                cmd += ["--local"]
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
            fullcmd = rcEnv.python_cmd + [os.path.join(rcEnv.paths.pathlib, "nodemgr.py")] + cmd

        thr.log_request("run '%s'" % " ".join(fullcmd), nodename, **kwargs)
        if options.sync:
            proc = Popen(fullcmd, stdout=PIPE, stderr=PIPE, stdin=None, close_fds=True)
            out, err = proc.communicate()
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
                "info": "started node action %s" % " ".join(cmd),
            }
        return result

