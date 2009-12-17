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
import os, sys
import select
import logging
from subprocess import *

def check_privs():
    if os.getuid() != 0:
        print 'Insufficient privileges. Try:\n sudo ' + ' '.join(sys.argv)
        sys.exit(1)


def banner(text, ch='=', length=78):
    spaced_text = ' %s ' % text
    banner = spaced_text.center(length, ch)
    return banner

def is_exe(fpath):
    """Returns True if file path is executable, False otherwize
    """
    return os.path.exists(fpath) and os.access(fpath, os.X_OK)

def which(program):
    """Returns True if program is in PATH and executable, False
    otherwize
    """
    fpath, fname = os.path.split(program)
    if fpath and is_exe(program):
        return program
    for path in os.environ["PATH"].split(os.pathsep):
        exe_file = os.path.join(path, program)
        if is_exe(exe_file):
            return exe_file
    return None

def call(argv=['/bin/false'], log=None, info=False, errlog=True):
    if log == None:
        log = logging.getLogger('CALL')
    if not argv:
        return (0, '')
    if info:
        log.info(' '.join(argv))
    else:
        log.debug(' '.join(argv))
    process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=True)
    buff = process.communicate()
    if errlog and len(buff[1]) > 0:
        log.error('error:\n' + buff[1])
    if len(buff[0]) > 0:
        log.debug('output:\n' + buff[0])

    return (process.returncode, buff[0])

def qcall(argv=['/bin/false']) :
    """qcall Launch Popen it args disgarding output and stderr"""
    if not argv:
        return (0, '')
    process = Popen(argv, stdout=PIPE, stderr=PIPE, close_fds=True)
    process.communicate()
    return process.returncode

def vcall(argv=['/bin/false'], log=None):
    return call(argv, log, True)

def getmount(path):
    path = os.path.abspath(path)
    while path != os.path.sep:
        if not os.path.islink(path) and os.path.ismount(path):
            return path
        path = os.path.abspath(os.path.join(path, os.pardir))
    return path

def protected_mount(path):
    if getmount(path) in ['/', '/usr', '/var', '/sys', '/proc', '/tmp', '/opt', '/dev', '/dev/pts', '/home', '/boot', '/dev/shm']:
        return True
    return False


if __name__ == "__main__":
    print "call(('id','-a'))"
    (r,output)=call(("/usr/bin/id","-a"))
    print "status: ",r,"output:",output

