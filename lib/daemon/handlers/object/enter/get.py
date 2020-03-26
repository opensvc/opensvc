import os
import subprocess
import time
import traceback
import uuid

import daemon.handlers.handler as handler
import daemon.shared as shared
import core.exceptions as ex
from rcGlobalEnv import rcEnv
from utilities.proc import which
from utilities.string import try_decode

MAX_TIMEOUT = 60

class Handler(handler.Handler):
    """
    Return a url a client browser can connect to for 5 seconds to enter the
    container specified by <rid> of the object specified by <path>.

    The url path is random.
    The basic auth credential is random.
    The port is random.
    """
    routes = (
        ("GET", "object_enter"),
    )
    access = {
        "roles": ["admin"],
        "namespaces": "FROM:path",
    }
    prototype = [
        {
            "name": "path",
            "desc": "The object path.",
            "required": True,
            "format": "object_path",
        },
        {
            "name": "rid",
            "desc": "The resource id of the container to enter.",
            "required": True,
            "format": "string",
            "example": "container#1",
        },
        {
            "name": "timeout",
            "desc": "The time the tty server will stay alive waiting for a client. Maximum %d seconds." % MAX_TIMEOUT,
            "required": False,
            "default": 5,
            "format": "duration",
            "example": "30s",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        timeout = options.timeout if options.timeout and options.timeout < MAX_TIMEOUT else MAX_TIMEOUT
        if not which("gotty"):
            raise ex.HTTP(500, "The gotty executable is not installed")
        creds = "user:" + str(uuid.uuid4())
        private_key = os.path.join(rcEnv.paths.certs, "private_key")
        cert_chain = os.path.join(rcEnv.paths.certs, "certificate_chain")
        cmd = [
            "gotty",
            "--port", "0",
            "--random-url",
            "--tls",
            "--tls-crt", cert_chain,
            "--tls-key", private_key,
            "--timeout", str(timeout),
            "--once",
            "--ws-origin", ".*",
            "--permit-write",
            "om", options.path, "enter", "--rid", options.rid,
        ]
        env = dict(os.environ).update({
            "GOTTY_CREDENTIAL": creds,
        })
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        thr.parent.push_proc(proc)
        for line in iter(proc.stderr.readline, ""):
            line = try_decode(line)
            if "https://" not in line:
                continue
            url = line.split("https://::", 1)[-1].strip()
            url = "https://" + creds + "@" + rcEnv.nodename + url
            return {"data": {"url": url}}
