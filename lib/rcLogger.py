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

try:
    type(PermissionError)
except:
    PermissionError = IOError

import platform
import re
from rcColor import colorize, color

DEFAULT_HANDLERS = ["file", "stream", "syslog"]

class ColorStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        logging.StreamHandler.__init__(self, stream)

    def format(self, record):
        text = logging.StreamHandler.format(self, record)
        def c(line):
            l = line.rstrip("\n").split(" - ")
            if len(l) < 3:
                return line

            l[0] = namefmt % l[0]
            l[0] = colorize(l[0], color.BOLD)
            l[1] = l[1].replace("ERROR", colorize("E", color.RED))
            l[1] = l[1].replace("WARNING", colorize("W", color.BROWN))
            l[1] = l[1].replace("DEBUG", colorize("D", color.LIGHTBLUE))
            l[1] = l[1].replace("INFO", " ")
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

def set_namelen(svcs=[], force=None):
    global namelen
    global namefmt

    maxlen = min_name_len
    for svc in svcs:
        if svc.disabled:
            continue
        svc_len = len(svc.svcname)

        # init with the "scheduler" length
        max_res_len = 10

        for r in svc.resources_by_id.values():
            if r is None:
                continue
            if r.is_disabled():
                continue
            l = len(r.rid)
            if r.subset:
                l += len(r.subset) + 2
            if l > max_res_len:
                max_res_len = l
        svc_len += max_res_len
        if svc_len > maxlen:
            maxlen = svc_len
    maxlen += len(rcEnv.nodename) + 1
    if force:
        maxlen = force
    namelen = maxlen
    namefmt = "%-"+str(namelen)+"s"

def initLogger(name, handlers=None):
    if handlers is None:
        handlers = DEFAULT_HANDLERS

    if name == rcEnv.nodename:
        logfile = os.path.join(rcEnv.paths.pathlog, "node") + '.log'
        debuglogfile = os.path.join(rcEnv.paths.pathlog, "node") + '.debug.log'
    else:
        if name.startswith(rcEnv.nodename):
            _name = name.replace(rcEnv.nodename+".", "", 1)
        else:
            _name = name
        logfile = os.path.join(rcEnv.paths.pathlog, _name) + '.log'
        debuglogfile = os.path.join(rcEnv.paths.pathlog, _name) + '.debug.log'
    log = logging.getLogger(name)
    log.propagate = False
    log.handlers = []

    if "file" in handlers:
        try:
            fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            filehandler = logging.handlers.RotatingFileHandler(logfile,
                                                               maxBytes=1*5242880,
                                                               backupCount=1)
            filehandler.setFormatter(fileformatter)
            filehandler.setLevel(logging.INFO)
            log.addHandler(filehandler)
        except PermissionError:
            pass

    if "stream" in handlers:
        streamformatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
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
            config.read(rcEnv.paths.nodeconf)
        except:
            pass
        try:
            facility = config.get("syslog", "facility")
        except:
            facility = "daemon"
        try:
            lvl = config.get("syslog", "level").upper()
            lvl = getattr(logging, lvl)
        except:
            lvl = logging.INFO
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
            sysloghandler.setLevel(lvl)
            sysloghandler.setFormatter(syslogformatter)
            log.addHandler(sysloghandler)

    if "file" in handlers:
        try:
            fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            filehandler = logging.handlers.RotatingFileHandler(debuglogfile,
                                                               maxBytes=3*5242880,
                                                               backupCount=1)
            filehandler.setFormatter(fileformatter)
            filehandler.setLevel(logging.DEBUG)
            log.addHandler(filehandler)
        except PermissionError:
            pass

    log.setLevel(logging.DEBUG)
    return log
