"""
The module defining the app.forking resource class.
"""

from .. import App, KEYWORDS as BASE_KEYWORDS
from core.objects.svcdict import KEYS

DRIVER_GROUP = "app"
DRIVER_BASENAME = "forking"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "script",
        "at": True,
        "required": False,
        "text": "Full path to the app launcher script. Or its basename if the file is hosted in the ``<pathetc>/<namespace>/<kind>/<name>.d/`` path. This script must accept as arg0 the activated actions word: ``start`` for start, ``stop`` for stop, ``status`` for check, ``info`` for info."
    },
    {
        "keyword": "status_log",
        "at": True,
        "required": False,
        "default": False,
        "convert": "boolean",
        "text": "Redirect the checker script stdout to the resource status info-log, and stderr to warn-log. The default is ``false``, for it is common the checker scripts outputs are not tuned for opensvc."
    },
    {
        "keyword": "check_timeout",
        "convert": "duration",
        "at": True,
        "text": "Wait for <duration> before declaring the app launcher check action a failure. Takes precedence over :kw:`timeout`. If neither :kw:`timeout` nor :kw:`check_timeout` is set, the agent waits indefinitely for the app launcher to return. A timeout can be coupled with :kw:`optional=true` to not abort a service instance check when an app launcher did not return.",
        "example": "180"
    },
    {
        "keyword": "info_timeout",
        "convert": "duration",
        "at": True,
        "text": "Wait for <duration> before declaring the app launcher info action a failure. Takes precedence over :kw:`timeout`. If neither :kw:`timeout` nor :kw:`info_timeout` is set, the agent waits indefinitely for the app launcher to return. A timeout can be coupled with :kw:`optional=true` to not abort a service instance info when an app launcher did not return.",
        "example": "180"
    },
    {
        "keyword": "start",
        "at": True,
        "default": False,
        "text": "``true`` execute :cmd:`<script> start` on start action. ``false`` do nothing on start action. ``<shlex expression>`` execute the command on start.",
    },
    {
        "keyword": "stop",
        "at": True,
        "default": False,
        "text": "``true`` execute :cmd:`<script> stop` on stop action. ``false`` do nothing on stop action. ``<shlex expression>`` execute the command on stop action.",
    },
    {
        "keyword": "check",
        "at": True,
        "default": False,
        "text": "``true`` execute :cmd:`<script> status` on status evaluation. ``false`` do nothing on status evaluation. ``<shlex expression>`` execute the command on status evaluation.",
    },
    {
        "keyword": "info",
        "at": True,
        "default": False,
        "text": "``true`` execute :cmd:`<script> info` on info action. ``false`` do nothing on info action. ``<shlex expression>`` execute the command on info action.",
    },
    {
        "keyword": "limit_as",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_cpu",
        "convert": "duration",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_core",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_data",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_fsize",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_memlock",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_nofile",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_nproc",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_rss",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_stack",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_vmem",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "user",
        "at": True,
        "text": "If the binary is owned by the root user, run it as the specified user instead of root."
    },
    {
        "keyword": "group",
        "at": True,
        "text": "If the binary is owned by the root user, run it as the specified group instead of root."
    },
    {
        "keyword": "cwd",
        "at": True,
        "text": "Change the working directory to the specified location instead of the default ``<pathtmp>``."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from env import Env
    data = []
    if Env.sysname == "Windows":
        return data
    data.append("app.forking")
    return data

class AppForking(App):
    """
    The forking App resource driver class.
    """

    def __init__(self, **kwargs):
        super(AppForking, self).__init__(type="app.forking", **kwargs)
