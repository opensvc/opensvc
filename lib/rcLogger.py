import sys
import os
import gzip
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

import extconfig
from rcColor import colorize, color

DEFAULT_HANDLERS = ["file", "stream", "syslog"]

def namer(name):
    return name + ".gz"

def rotator(source, dest):
    with open(source, "rb") as sf:
        data = sf.read()
        with gzip.open(dest, "wb") as df:
            df.write(data)
    os.remove(source)

class StreamFilter(logging.Filter):
    def filter(self, record):
        try:
            if record.args["f_stream"] is False:
                return False
            else:
                return True
        except (KeyError, AttributeError, TypeError):
            return True

class RedactingFormatter(object):
    def __init__(self, orig_formatter):
        self.orig_formatter = orig_formatter

    def format(self, record):
        msg = self.orig_formatter.format(record)
        for pattern in extconfig.SECRETS:
            msg = msg.replace(pattern, "xxxx")
        return msg

    def __getattr__(self, attr):
        return getattr(self.orig_formatter, attr)

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
            if l[2].startswith("do "):
                l[2] = colorize(l[2], color.BOLD)
            return " ".join(l)

        return c(text)

class LoggerHandler(logging.handlers.SysLogHandler):
    def __init__(self, facility=logging.handlers.SysLogHandler.LOG_USER):
        logging.Handler.__init__(self)
        self.formatter = None
        self.facility = facility.upper()

    def close(self):
        pass

    def emit(self, record):
        """
        Emit a record.

        The record is formatted, and then sent to the syslog server. If
        exception information is present, it is NOT sent to the server.
        """
        try:
            import syslog
            facility = syslog.__dict__["LOG_"+self.facility]
            syslog.openlog("opensvc", 0, facility)
        except Exception as exc:
            self.handleError(record)
        try:
            prio = self.priority_names[self.mapPriority(record.levelname)]
            msg = self.format(record)
            syslog.syslog(prio, msg)
        except Exception as exc:
            self.handleError(record)
        finally:
            syslog.closelog()

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
    else:
        if name.startswith(rcEnv.nodename):
            _name = name.replace(rcEnv.nodename+".", "", 1)
        else:
            _name = name
        logfile = os.path.join(rcEnv.paths.pathlog, _name) + '.log'
    log = logging.getLogger(name)
    log.propagate = False
    log.handlers = []

    if "file" in handlers:
        try:
            fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            filehandler = logging.handlers.RotatingFileHandler(logfile,
                                                               maxBytes=1*5242880,
                                                               backupCount=1)
            filehandler.setFormatter(RedactingFormatter(fileformatter))
            filehandler.setLevel(logging.INFO)
            filehandler.rotator = rotator
            filehandler.namer = namer
            log.addHandler(filehandler)

            if '--debug' in sys.argv:
                filehandler.setLevel(logging.DEBUG)
        except PermissionError:
            pass

    if "stream" in handlers:
        streamformatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
        streamhandler = ColorStreamHandler()
        streamhandler.setFormatter(RedactingFormatter(streamformatter))
        streamfilter = StreamFilter()
        streamhandler.addFilter(streamfilter)
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
        from six.moves import configparser as ConfigParser
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

        syslogformatter = logging.Formatter("%(name)s %(message)s")
        try:
            if rcEnv.sysname == "SunOS" and not isinstance(address, tuple):
                sysloghandler = LoggerHandler(facility=facility)
            else:
                sysloghandler = logging.handlers.SysLogHandler(address=address, facility=facility)
        except Exception as e:
            sysloghandler = None
        if sysloghandler:
            sysloghandler.setLevel(lvl)
            sysloghandler.setFormatter(RedactingFormatter(syslogformatter))
            log.addHandler(sysloghandler)

    log.setLevel(logging.DEBUG)

    return log


