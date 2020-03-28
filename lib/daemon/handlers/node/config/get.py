import codecs
import os
import traceback

import daemon.handler
import daemon.shared as shared
from env import Env

class Handler(daemon.handler.BaseHandler):
    """
    Return the node private configuration.
    """
    routes = (
        ("GET", "node_config"),
        (None, "get_node_config"),
    )
    prototype = [
        {
            "name": "format",
            "desc": "The data format to provide.",
            "candidates": ["json", "ini"],
            "default": "ini",
            "required": False,
            "format": "string",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.format == "json":
            return self._node_config_json(nodename, thr=thr, **kwargs)
        else:
            return self._node_config_file(nodename, thr=thr, **kwargs)

    def _node_config_json(self, nodename, thr=None, **kwargs):
        try:
            return shared.NODE.print_config_data()
        except Exception as exc:
            return {"status": "1", "error": str(exc), "traceback": traceback.format_exc()}

    def _node_config_file(self, nodename, thr=None, **kwargs):
        fpath = os.path.join(Env.paths.pathetc, "node.conf")
        if not os.path.exists(fpath):
            return {"error": "%s does not exist" % fpath, "status": 3}
        mtime = os.path.getmtime(fpath)
        with codecs.open(fpath, "r", "utf8") as filep:
            buff = filep.read()
        thr.log.info("serve node config to %s", nodename)
        return {"status": 0, "data": buff, "mtime": mtime}

