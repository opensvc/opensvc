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
# To change this template, choose Tools | Templates
# and open the template in the editor.

class excEncapUnjoignable(Exception):
    pass

class excError(Exception):
    """ Failed action
    """
    def __init__(self, value=""):
        self.value = value
    def __str__(self):
        return str(self.value)

class OptNotFound(Exception):
    """ Service config file option not found
    """

class excSignal(Exception):
    """ Termination signal received
    """

class excUndefined(Exception):
    """ Mandatory Undefined action exception
    """
    def __init__(self,action=None,className=None,func=None):
        self.action=action
        self.className=className
        self.func=func
    def __str__(self):
        return "Undefined mandatory Action %s for className %s in function %s" % \
                (self.action,self.className,self.func)

class syncNoNodesToSync(Exception):
    """ No nodes to sync => abort stacked syncs resource actions
    """

class syncNoFilesToSync(Exception):
    """ No files to sync => move on to the next stacked syncs resource actions
    """

class syncConfigSyntaxError(Exception):
    """ Bogus configuration syntax => abort all
    """

class syncNotSnapable(Exception):
    """ A dir/file specified as source of a sync with snap is not included in a 
        snapable resource mount => abort all
    """

class syncSnapExists(Exception):
    """ The snapshot already exists
    """

class syncSnapCreateError(Exception):
    """ Error in snapshot creation => clean up
    """

class syncSnapDestroyError(Exception):
    """ Error in snapshot destroy => clean up
    """

class syncSnapMountError(Exception):
    """ Error mounting fs => clean up
    """

class excAbortAction(Exception):
    """ Abort multi-resource action
    """

class excInitError(Exception):
    """ Resource initialisation error
    """

class excScsiPrNotsupported(Exception):
    """ Scsi persistent reservation is not supported
    """

class excNotAvailable(Exception):
    """ Not available
    """

class excNotSupported(Exception):
    """ Not supported
    """

class excBug(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class MissImpl(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpDevDown(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpConflict(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpAlreadyUp(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpNoActions(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
