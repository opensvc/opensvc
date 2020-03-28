import daemon.handler

class Handler(daemon.handler.BaseHandler):
    """
    Return the api handlers manifest.
    """
    routes = (
        ("GET", "api"),
        (None, "get_api"),
    )
    prototype = []
    access = {}

    def action(self, nodename, thr=None, **kwargs):
        sigs = []
        data = []
        for h in thr.parent.handlers.values():
            sig = h.routes[0]
            if sig in sigs:
                continue
            sigs.append(sig)
            data.append({
                "routes": [{"method": r[0], "path": r[1]} for r in h.routes],
                "prototype": h.prototype,
                "access": h.access,
                "desc": h.__doc__.strip(),
                "stream": h.stream,
                "multiplex": h.multiplex,
            })
        return data

