import handler

class Handler(handler.Handler):
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
        data = {
            "name": "root" if thr.usr is False else thr.usr.name,
            "namespace": None if thr.usr is False else thr.usr.namespace,
            "auth": thr.usr_auth,
            "raw_grant": "root" if thr.usr is False else thr.usr.oget("DEFAULT", "grant"),
            "grant": dict((k, list(v) if v is not None else None) for k, v in thr.usr_grants.items()),
        }
        return data

