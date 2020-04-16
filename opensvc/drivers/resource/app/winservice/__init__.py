"""
The module defining the app.simple resource class.
"""

try:
    import win32serviceutil
except ImportError:
    pass

import core.exceptions as ex

from .. import App, KEYWORDS as BASE_KEYWORDS, StatusNA, StatusWARN
from core.objects.svcdict import KEYS


DEFAULT_TIMEOUT = 300

SERVICE_STOPPED = 1
SERVICE_START_PENDING = 2
SERVICE_STOP_PENDING = 3
SERVICE_RUNNING = 4
SERVICE_CONTINUE_PENDING = 5
SERVICE_PAUSE_PENDING = 6
SERVICE_PAUSED = 7

STATUS_STR = {
    1: "STOPPED",
    2: "START_PENDING",
    3: "STOP_PENDING",
    4: "RUNNING",
    5: "CONTINUE_PENDING",
    6: "PAUSE_PENDING",
    7: "PAUSED",
}

DRIVER_GROUP = "app"
DRIVER_BASENAME = "winservice"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "name",
        "at": True,
        "text": "The name of the Windows service."
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
    if Env.sysname != "Windows":
        return data
    data.append("app.winservice")
    return data


class AppWinservice(App):
    """
    The app.winservice resource driver class.
    """

    def __init__(self, name=None, **kwargs):
        self.name = name
        super(AppWinservice, self).__init__(type="app.winservice", **kwargs)

    def get_state(self):
        """
        typedef struct _SERVICE_STATUS {
          DWORD dwServiceType;
          DWORD dwCurrentState;
          DWORD dwControlsAccepted;
          DWORD dwWin32ExitCode;
          DWORD dwServiceSpecificExitCode;
          DWORD dwCheckPoint;
          DWORD dwWaitHint;
        } SERVICE_STATUS, *LPSERVICE_STATUS;
        """
        _, state, _, _, _, _, _ = win32serviceutil.QueryServiceStatus(self.name)
        return state

    def get_timeout(self, action):
        timeout = super(AppWinservice, self).get_timeout(action)
        if timeout is None:
            return DEFAULT_TIMEOUT
        return timeout

    def wait_for_state(self, timeout, target, transition):
        def fn():
            state = self.get_state()
            if state == target:
                return True
            if state != transition:
                raise ex.Error("unexpected state: %s" % STATUS_STR[state])
            return False
        self.wait_for_fn(fn, timeout, 1)

    def _check(self):
        if self.name is None:
            raise StatusNA
        state = self.get_state()
        if state == SERVICE_RUNNING:
            return 0
        if state == SERVICE_STOPPED:
            return 1
        self.status_log(STATUS_STR[state])
        raise StatusWARN

    def stop(self):
        if self.name is None:
            return
        try:
            if self.is_up() == 1:
                self.log.info("already down")
                return
        except StatusNA:
            self.log.info("skip, no name set")
            return
        except StatusWARN:
            pass
        self.log.info("stop winservice %s", self.name)
        win32serviceutil.StopService(self.name)
        timeout = self.get_timeout("stop")
        self.wait_for_state(timeout, SERVICE_STOPPED, SERVICE_STOP_PENDING)

    def start(self):
        if self.name is None:
            return
        try:
            if self.is_up() == 0:
                self.log.info("already up")
                return
        except StatusNA:
            self.log.info("skip, no name set")
            return
        except StatusWARN:
            pass
        self.log.info("start winservice %s", self.name)
        win32serviceutil.StartService(self.name)
        timeout = self.get_timeout("start")
        self.wait_for_state(timeout, SERVICE_RUNNING, SERVICE_START_PENDING)
