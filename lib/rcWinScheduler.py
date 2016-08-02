'''

 Author: Alex Baker
 Date: 7th July 2008
 Description : Simple python program to generate wrap as a service based on example on the web, see link below.

 http://essiene.blogspot.com/2005/04/python-windows-services.html

 Usage : python aservice.py install
 Usage : python aservice.py start
 Usage : python aservice.py stop
 Usage : python aservice.py remove

 C:\>python aservice.py  --username <username> --password <PASSWORD> --startup auto install

'''


import win32service
import win32serviceutil
import win32api
import win32con
import win32event
import win32evtlogutil
import os
import servicemanager

import datetime
from subprocess import *

import sys
from rcListener import listener

class OsvcSched(win32serviceutil.ServiceFramework):

    _svc_name_ = "OsvcSched"
    _svc_display_name_ = "OpenSVC job scheduler"
    _svc_description_ = "Schedule the OpenSVC jobs"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        now = datetime.datetime.now()
        self.next_task10 = now + datetime.timedelta(minutes=1)


    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
	sys.stop_listener = True
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,servicemanager.PYS_SERVICE_STARTED,(self._svc_name_, ''))
        a = listener()

        self.timeout = 60000

        while 1:
            # Wait for service stop signal, if I timeout, loop again
            rc = win32event.WaitForSingleObject(self.hWaitStop, self.timeout)
            # Check to see if self.hWaitStop happened
            if rc == win32event.WAIT_OBJECT_0:
                # Stop signal encountered
                servicemanager.LogInfoMsg("%s - STOPPED"%self._svc_name_)
                break
            else:
                #servicemanager.LogInfoMsg("%s - ALIVE"%self._svc_name_)
                self.SvcDoJob()

    def SvcDoJob(self):
        now = datetime.datetime.now()
        if now > self.next_task10:
            self.run_task10()
            self.next_task10 = now + datetime.timedelta(minutes=10)

    def run_task10(self):
        cmd = [rcEnv.svcmon, "--updatedb"]
        servicemanager.LogInfoMsg("run %s" % ' '.join(cmd))
        p = Popen(cmd, stdout=None, stderr=None, stdin=None)
        p.communicate()
        servicemanager.LogInfoMsg("run internal scheduler")
        cmd = [rcEnv.cron]
        p = Popen(cmd, stdout=None, stderr=None, stdin=None)
        p.communicate()

def ctrlHandler(ctrlType):
    return True

if __name__ == '__main__':
    win32api.SetConsoleCtrlHandler(ctrlHandler, True)
    win32serviceutil.HandleCommandLine(OsvcSched)

