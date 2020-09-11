import os
from copy import deepcopy
from subprocess import Popen, PIPE

import daemon.handler
from daemon.handler_action_helper import get_action_args_from_parser
from env import Env
from utilities.string import bdecode


class Handler(daemon.handler.BaseHandler):
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
            "desc": "If true, adds --local if not already present in <options>.",
            "required": False,
            "default": True,
            "format": "boolean",
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
            if opt in options.options and options.action not in ("daemon_join", "daemon_rejoin"):
                del options.options[opt]
        if options.action_mode and options.options.get("local"):
            if "local" in options.options:
                del options.options["local"]
        for opt, ropt in (("jsonpath_filter", "filter"),):
            if opt in options.options:
                options.options[ropt] = options.options[opt]
                del options.options[opt]
        options.options["local"] = True

        if options.action.startswith("daemon_"):
            subsystem = "daemon"
            action = options.action[7:]
            from commands.daemon.parser import OPT
        elif options.action.startswith("net_"):
            subsystem = "net"
            action = options.action[4:]
            from commands.network.parser import OPT
        elif options.action.startswith("network_"):
            subsystem = "net"
            action = options.action[8:]
            from commands.network.parser import OPT
        elif options.action.startswith("pool_"):
            subsystem = "pool"
            action = options.action[5:]
            from commands.pool.parser import OPT
        else:
            subsystem = "node"
            action = options.action
            from commands.node.parser import OPT

        cmd = get_action_args_from_parser(OPT, action, options)
        fullcmd = Env.om + [subsystem] + cmd

        thr.log_request("run 'om %s %s'" % (subsystem, " ".join(cmd)), nodename, **kwargs)
        new_env = deepcopy(os.environ)
        if new_env.get('LOGNAME') is None:
            new_env['LOGNAME'] = "root"
        if options.sync:
            proc = Popen(fullcmd, stdout=PIPE, stderr=PIPE, stdin=None, close_fds=True, env=new_env)
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
            import uuid
            session_id = str(uuid.uuid4())
            new_env["OSVC_PARENT_SESSION_UUID"] = session_id
            proc = Popen(fullcmd, stdin=None, close_fds=True, env=new_env)
            thr.parent.push_proc(proc, cmd=fullcmd, session_id=session_id)
            result = {
                "status": 0,
                "data": {
                    "pid": proc.pid,
                    "session_id": session_id,
                },
                "info": "started node action %s" % " ".join(cmd),
            }
        return result
