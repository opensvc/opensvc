#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import sys
import logging
import logging.handlers
from rcGlobalEnv import *

def initLogger(name):
    log = logging.getLogger(name)
    name_width = len(name) + 8

    """Common log formatter
    """
    fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    streamformatter = logging.Formatter("%(asctime)s %(levelname)-7s %(name)-"+str(name_width)+"s %(message)s", datefmt="%H:%M:%S")

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

    return log
