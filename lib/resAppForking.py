"""
The module defining the app.forking resource class.
"""

import resApp
from svcBuilder import init_kwargs


def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["script"] = svc.oget(s, "script")
    kwargs["start"] = svc.oget(s, "start")
    kwargs["stop"] = svc.oget(s, "stop")
    kwargs["check"] = svc.oget(s, "check")
    kwargs["info"] = svc.oget(s, "info")
    kwargs["status_log"] = svc.oget(s, "status_log")
    kwargs["timeout"] = svc.oget(s, "timeout")
    kwargs["start_timeout"] = svc.oget(s, "start_timeout")
    kwargs["stop_timeout"] = svc.oget(s, "stop_timeout")
    kwargs["check_timeout"] = svc.oget(s, "check_timeout")
    kwargs["info_timeout"] = svc.oget(s, "info_timeout")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["group"] = svc.oget(s, "group")
    kwargs["cwd"] = svc.oget(s, "cwd")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    r = App(**kwargs)
    svc += r


class App(resApp.App):
    """
    The forking App resource driver class.
    """

    def __init__(self, rid, **kwargs):
        resApp.App.__init__(self, rid, type="app.forking", **kwargs)
