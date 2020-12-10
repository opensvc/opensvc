import traceback

import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the node checkers data (filesystem usage, ...).
    """
    routes = (
        ("GET", "node_checks"),
    )
    prototype = [
        {
            "name": "checkers",
            "desc": "The list of checkers to provide data from: btrfs, eth, "
                    "fm, fs_i, fs_u, jstat, lag, mcelog, mpath, numa, raid, "
                    "sync, vg_u, zpool",
            "required": False,
            "format": "list",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        try:
            drvs = shared.NODE.checks_drivers(checkers=options.checkers)
            return drvs.do_checks()
        except Exception as exc:
            return {"status": "1", "error": str(exc), "traceback": traceback.format_exc()}

