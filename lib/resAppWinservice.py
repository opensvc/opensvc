"""
The module defining the app.simple resource class.
"""

try:
    import win32serviceutil
except ImportError:
    raise

import resApp
import rcStatus
import rcExceptions as ex
from rcGlobalEnv import rcEnv

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

class App(resApp.App):
    """
    The app.winservice resource driver class.
    """

    def __init__(self, rid, name=None, **kwargs):
        self.name = name
        resApp.App.__init__(self, rid, type="app.winservice", **kwargs)

    def _check(self):
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
        if self.name is None:
            raise resApp.StatusNA
        _, state, _, _, _, _, _ = win32serviceutil.QueryServiceStatus(self.name)
        if state == SERVICE_RUNNING:
            return 0
        if state == SERVICE_STOPPED:
            return 1
        self.status_log(STATUS_STR[state])
        raise resApp.StatusWARN

    def stop(self):
        if self.name is None:
            return
        try:
            if self.is_up() == 1:
                self.log.info("already down")
                return
        except resApp.StatusNA:
            self.log.info("skip, no name set")
            return
        except resApp.StatusWARN:
            pass
        self.log.info("stop winservice %s", self.name)
        win32serviceutil.StopService(self.name)

    def start(self):
        if self.name is None:
            return
        try:
            if self.is_up() == 0:
                self.log.info("already up")
                return
        except resApp.StatusNA:
            self.log.info("skip, no name set")
            return
        except resApp.StatusWARN:
            pass
        self.log.info("start winservice %s", self.name)
        win32serviceutil.StartService(self.name)

