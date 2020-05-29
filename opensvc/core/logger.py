import gzip
import logging
import logging.handlers
import os
import sys

import foreign.six as six
from env import Env
from utilities.files import makedirs

min_name_len = 10
namelen = 10
namefmt = "%-"+str(namelen)+"s"

try:
    type(PermissionError)
except:
    PermissionError = IOError

import core.extconfig
from utilities.render.color import colorize, color

DEFAULT_HANDLERS = ["file", "stream", "syslog"]

def namer(name):
    """
    Adds a .gz suffix to the rotated file.
    """
    return name + ".gz"

def rotator(source, dest):
    """
    Compress file upon rotation.
    """
    with open(source, "rb") as sf:
        data = sf.read()
        with gzip.open(dest, "wb") as df:
            df.write(data)
    os.remove(source)

class StreamFilter(logging.Filter):
    """
    Discard from StreamHander records flagged f_stream=False.
    """
    def filter(self, record):
        try:
            if record.args["f_stream"] is False:
                return False
            else:
                return True
        except (KeyError, AttributeError, TypeError):
            return True

class OsvcFormatter(logging.Formatter):
    """
    Add context information embedded in the record via "extra".
    And obfuscate obvious passwords seen in the record message.
    If human is True, factorize the context information, trim the level and colorize.
    """
    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)
        self.sid = False
        self.human = False
        self.last_context = None
        self.attrs = [
            ("sid", "sid"),
            ("node", "n"),
            ("component", "c"),
            ("path", "o"),
            ("subset", "rs"),
            ("rid", "r"),
            ("cron", "sc"),
        ]

    def format(self, record):
        record.message = record.getMessage()
        record.context = ""
        for xattr, key in self.attrs:
            if xattr == "sid" and not self.sid:
                continue
            try:
                val = getattr(record, xattr)
                if val in (None, ""):
                    continue
                if val is True:
                    val = "y"
                elif val is False:
                    val = "n"
                record.context += "%s:%s " % (key, val)
            except AttributeError:
                pass
        record.context = record.context.rstrip()
        for pattern in core.extconfig.SECRETS:
            record.message = record.message.replace(pattern, "xxxx")

        if not self.human:
            return logging.Formatter.format(self, record)

        # Factorize context information, trim the level and colorize.
        buff = ""
        if self.last_context != record.context:
            buff += colorize("@ " + record.context, color.LIGHTBLUE)+"\n"
            self.last_context = record.context
        if record.levelname == "INFO":
            buff += "  " + record.message
        elif record.levelname == "ERROR":
            buff += colorize("E " + record.message, color.RED)
        elif record.levelname == "WARNING":
            buff += colorize("W " + record.message, color.BROWN)
        elif record.levelname == "DEBUG":
            buff += colorize("D " + record.message, color.LIGHTBLUE)
        return buff

class OsvcFileHandler(logging.handlers.RotatingFileHandler):
    """
    Create the hosting directory and setup a RotatingFileHandler.
    """
    def __init__(self, logfile):
        logdir = os.path.dirname(logfile)
        makedirs(logdir)
        logging.handlers.RotatingFileHandler.__init__(self, logfile, maxBytes=1*5242880, backupCount=1)

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
            if six.PY2:
                msg = msg.encode("utf8", errors="ignore")
            syslog.syslog(prio, msg)
        except Exception as exc:
            self.handleError(record)
        finally:
            syslog.closelog()

def initLogger(root, logfile, handlers=None, sid=True):
    if handlers is None:
        handlers = DEFAULT_HANDLERS
    log = logging.getLogger(root)
    if log.handlers:
        # already setup
        return log
    log.propagate = False
    log.handlers = []

    if "file" in handlers:
        try:
            fileformatter = OsvcFormatter("%(asctime)s %(levelname)s %(context)s | %(message)s")
            fileformatter.sid = sid
            filehandler = OsvcFileHandler(logfile)
            filehandler.setFormatter(fileformatter)
            filehandler.rotator = rotator
            filehandler.namer = namer
            log.addHandler(filehandler)

            if '--debug' in sys.argv:
                filehandler.setLevel(logging.DEBUG)
            else:
                filehandler.setLevel(logging.INFO)

        except PermissionError:
            pass

    if "stream" in handlers:
        streamformatter = OsvcFormatter("%(levelname)s %(context)s %(message)s")
        streamformatter.human = True
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(streamformatter)
        streamfilter = StreamFilter()
        streamhandler.addFilter(streamfilter)
        log.addHandler(streamhandler)

        if '--debug' in sys.argv:
                Env.loglevel = logging.DEBUG
                streamhandler.setLevel(logging.DEBUG)
        else:
                Env.loglevel = logging.INFO
                streamhandler.setLevel(logging.INFO)

    if "syslog" in handlers:
        from foreign.six.moves import configparser as ConfigParser
        config = ConfigParser.RawConfigParser({})
        try:
            config.read(Env.paths.nodeconf)
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

        syslogformatter = OsvcFormatter("%(context)s %(message)s")
        try:
            if Env.sysname == "SunOS" and not isinstance(address, tuple):
                sysloghandler = LoggerHandler(facility=facility)
            else:
                sysloghandler = logging.handlers.SysLogHandler(address=address, facility=facility)
        except Exception as e:
            sysloghandler = None
        if sysloghandler:
            sysloghandler.setLevel(lvl)
            sysloghandler.setFormatter(syslogformatter)
            log.addHandler(sysloghandler)

    log.setLevel(logging.DEBUG)

    return log


