class HTTP(Exception):
    def __init__(self, status, msg=""):
        self.status = status
        self.msg = msg
    def __str__(self):
        return "status %s: %s" % (self.status, self.msg)

class excEncapUnjoinable(Exception):
    pass

class excError(Exception):
    """ Failed action
    """
    def __init__(self, value=""):
        self.value = value
    def __str__(self):
        return str(self.value)

class excVersion(Exception):
    """ propagate the version string
    """
    def __init__(self, value=""):
        self.value = value
    def __str__(self):
        return str(self.value)

class excAlreadyDone(Exception):
    pass

class OptNotFound(Exception):
    """
    Service config file option not found.
    Raised by Svc::conf_get().
    The default property is used to propagate the keyword default value.
    """
    def __init__(self, value="", default=None):
        self.value = value
        self.default = default
    def __str__(self):
        return "%s default: %s" % (str(self.value), repr(self.default))

class RequiredOptNotFound(Exception):
    """
    Required service config file option not found.
    Raised by Svc::conf_get().
    """
    pass

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

class excEndAction(Exception):
    """ End multi-resource action. Not an error.
    """

class excContinueAction(Exception):
    """ Abort current resource action, but proceed anyway
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
