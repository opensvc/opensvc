import sys
import os
import errno
import logging
import logging.handlers
from rcGlobalEnv import *
from subprocess import *

min_name_len = 8

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



def set_streamformatter(svcs):
    maxlen = get_max_name_len(svcs)
    streamformatter = logging.Formatter("%(levelname)-7s %(name)-"+str(maxlen)+"s %(message)s")
    for svc in svcs:
        handler = svc.log.handlers[1]
        handler.setFormatter(streamformatter)

def get_max_name_len(svcs):
    maxlen = min_name_len
    for svc in svcs:
        if svc.is_disabled():
            continue
        for r in svc.resources_by_id.values():
            if r is None:
                continue
            if r.is_disabled():
                continue
            l = len(r.log_label())
            if l > maxlen:
                maxlen = l
    return maxlen

def initLogger(name):
    log = logging.getLogger(name)
    if name in rcEnv.logging_initialized:
        return log

    """Common log formatter
    """
    fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    streamformatter = logging.Formatter("%(levelname)-7s %(name)s %(message)s")

    """Common logfile with rotation
    """
    filehandler = logging.handlers.RotatingFileHandler(rcEnv.logfile,
                                                       maxBytes=5242880,
                                                       backupCount=5)
    filehandler.setFormatter(fileformatter)
    log.addHandler(filehandler)

    """Stdout logger
    """
    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(streamformatter)
    log.addHandler(streamhandler)

    """ syslog
    """
    try:
        import ConfigParser
    except ImportError:
        import configparser as ConfigParser
    config = ConfigParser.RawConfigParser({})
    try:
        config.read("/opt/opensvc/etc/node.conf")
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
        sysloghandler.setFormatter(syslogformatter)
        log.addHandler(sysloghandler)

    if '--debug' in sys.argv:
            rcEnv.loglevel = logging.DEBUG
            log.setLevel(logging.DEBUG)
    elif '--warn' in sys.argv:
            rcEnv.loglevel = logging.WARNING
            log.setLevel(logging.WARNING)
    elif '--error' in sys.argv:
            rcEnv.loglevel = logging.ERROR
            log.setLevel(logging.ERROR)
    else:
            rcEnv.loglevel = logging.INFO
            log.setLevel(logging.INFO)

    rcEnv.logging_initialized.append(name)
    return log
