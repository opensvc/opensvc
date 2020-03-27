import daemon.handler

class Handler(daemon.handler.BaseHandler):
    """
    Return the authentified user information.
    """
    routes = (
        ("GET", "whoami"),
        (None, "whoami"),
    )
    prototype = []
    access = {}

    def action(self, nodename, thr=None, **kwargs):
        if thr.usr is None:
            name = "nobody"
            namespace = None
            auth = None
            raw_grant = ""
            grant = {}
        elif thr.usr is False:
            name = "root"
            namespace = None
            auth = thr.usr_auth
            raw_grant = "root"
            grant = {"root": None}
        else:
            name = thr.usr.name
            namespace = thr.usr.namespace
            auth = thr.usr_auth
            raw_grant = thr.usr.oget("DEFAULT", "grant")
            grant = dict((k, list(v) if v is not None else None) for k, v in thr.usr_grants.items())
        data = {
            "name": name,
            "namespace": namespace,
            "auth": auth,
            "raw_grant": raw_grant,
            "grant": grant,
        }
        return data

