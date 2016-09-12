import sys
import os
import errno
import logging
import logging.handlers
from rcGlobalEnv import rcEnv
from subprocess import *

min_name_len = 10
namelen = 10
namefmt = "%-"+str(namelen)+"s"
include_svcname = True

try:
    type(PermissionError)
except:
    PermissionError = IOError

import platform
import re
from rcColor import colorize, color

class ColorStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        logging.StreamHandler.__init__(self, stream)

    def format(self, record):
        text = logging.StreamHandler.format(self, record)
        def c(line):
            # remove date, keep time
            line = re.sub(r'^....-..-.. ', "", line)

            l = line.rstrip("\n").split(" - ")
            if len(l) < 3:
                return line

            if not include_svcname:
                l[1] = l[1].split(".")[-1]
                if "#" not in l[1] and l[1] != "scheduler":
                    l[1] = ""
            if len(l[1]) > namelen:
                l[1] = "*"+l[1][-(namelen-1):]
            l[1] = namefmt % l[1]
            l[1] = colorize(l[1], color.BOLD)
            l[2] = "%-7s" % l[2]
            l[2] = l[2].replace("ERROR", colorize("ERROR", color.RED))
            l[2] = l[2].replace("WARNING", colorize("WARNING", color.BROWN))
            l[2] = l[2].replace("INFO", colorize("INFO", color.LIGHTBLUE))
            return " ".join(l)

        return c(text)

class LoggerHandler(logging.handlers.SysLogHandler):
    def __init__(self, facility=logging.handlers.SysLogHandler.LOG_USER):
        logging.Handler.__init__(self)
        self.facility = facility
        self.formatter = None

    def close(self):
        pass

    def emit(self, record):
        try:
            msg = self.format(record)
            cmd = ["logger", "-t", "", "-p", self.facility+"."+record.levelname.lower(), msg]
            p = Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True)
            p.communicate()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

def set_namelen(svcs):
    global namelen
    global namefmt
    global include_svcname

    maxlen = min_name_len
    for svc in svcs:
        if svc.is_disabled():
            continue
        for r in svc.resources_by_id.values():
            if r is None:
                continue
            if r.is_disabled():
                continue
            l = len(r.rid)
            if r.subset:
                l += len(r.subset) + 1
            if len(svcs) > 1:
                include_svcname = True
                l += len(svc.svcname) + 1
            else:
                include_svcname = False
            if l > maxlen:
                maxlen = l
    namelen = maxlen
    namefmt = "%-"+str(namelen)+"s"

def initLogger(name, handlers=["file", "stream", "syslog"]):
    log = logging.getLogger(name)
    log.handlers = []

    if "file" in handlers:
        try:
            fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            filehandler = logging.handlers.RotatingFileHandler(rcEnv.logfile,
                                                               maxBytes=5242880,
                                                               backupCount=5)
            filehandler.setFormatter(fileformatter)
            filehandler.setLevel(logging.DEBUG)
            log.addHandler(filehandler)
        except PermissionError:
            pass

    if "stream" in handlers:
        streamformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        streamhandler = ColorStreamHandler()
        streamhandler.setFormatter(streamformatter)
        log.addHandler(streamhandler)

        if '--debug' in sys.argv:
                rcEnv.loglevel = logging.DEBUG
                streamhandler.setLevel(logging.DEBUG)
        elif '--warn' in sys.argv:
                rcEnv.loglevel = logging.WARNING
                streamhandler.setLevel(logging.WARNING)
        elif '--error' in sys.argv:
                rcEnv.loglevel = logging.ERROR
                streamhandler.setLevel(logging.ERROR)
        else:
                rcEnv.loglevel = logging.INFO
                streamhandler.setLevel(logging.INFO)

    if "syslog" in handlers:
        try:
            import ConfigParser
        except ImportError:
            import configparser as ConfigParser
        config = ConfigParser.RawConfigParser({})
        try:
            config.read(rcEnv.nodeconf)
        except:
            pass
        try:
            facility = config.get("syslog", "facility")
        except:
            facility = "daemon"
        try:
            host = config.get("syslog", "host")
        except:
            host = None
        try:
            port = int(config.get("syslog", "port"))
        except:
            port = None
        address = None
        if host is None and port is None:
            if os.path.exists("/dev/log"):
                address = os.path.realpath("/dev/log")
            elif os.path.exists("/var/run/syslog"):
                address = os.path.realpath("/var/run/syslog")
        if address is None:
            if host is None:
                host = "localhost"
            if port is None:
                port = 514
            address = (host, port)

        syslogformatter = logging.Formatter("opensvc: %(name)s %(message)s")
        try:
            sysloghandler = logging.handlers.SysLogHandler(address=address, facility=facility)
        except Exception as e:
            if e.errno == errno.ENOTSOCK:
                # solaris /dev/log is a stream device
                sysloghandler = LoggerHandler(facility=facility)
            else:
                sysloghandler = None
        if sysloghandler:
            sysloghandler.setLevel(logging.INFO)
            sysloghandler.setFormatter(syslogformatter)
            log.addHandler(sysloghandler)

    log.setLevel(logging.DEBUG)
    return log
