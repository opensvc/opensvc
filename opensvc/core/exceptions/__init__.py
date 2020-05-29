class OsvcException(Exception):
    pass

class HTTP(OsvcException):
    def __init__(self, status, msg=""):
        self.status = status
        self.msg = msg
    def __str__(self):
        return "status %s: %s" % (self.status, self.msg)

class EncapUnjoinable(OsvcException):
    pass

class Error(OsvcException):
    """ Failed action
    """
    def __init__(self, value=""):
        self.value = value
    def __str__(self):
        return str(self.value)

class Version(OsvcException):
    """ propagate the version string
    """
    def __init__(self, value=""):
        self.value = value
    def __str__(self):
        return str(self.value)

class TimeOut(OsvcException):
    pass

class AlreadyDone(OsvcException):
    pass

class OptNotFound(OsvcException):
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

class RequiredOptNotFound(OsvcException):
    """
    Required service config file option not found.
    Raised by Svc::conf_get().
    """
    pass

class Signal(OsvcException):
    """ Termination signal received
    """

class Undefined(OsvcException):
    """ Mandatory Undefined action exception
    """
    def __init__(self,action=None,className=None,func=None):
        self.action=action
        self.className=className
        self.func=func
    def __str__(self):
        return "Undefined mandatory Action %s for className %s in function %s" % \
                (self.action,self.className,self.func)

class syncNoNodesToSync(OsvcException):
    """ No nodes to sync => abort stacked syncs resource actions
    """

class syncNoFilesToSync(OsvcException):
    """ No files to sync => move on to the next stacked syncs resource actions
    """

class syncConfigSyntaxError(OsvcException):
    """ Bogus configuration syntax => abort all
    """

class syncNotSnapable(OsvcException):
    """ A dir/file specified as source of a sync with snap is not included in a
        snapable resource mount => abort all
    """

class syncSnapExists(OsvcException):
    """ The snapshot already exists
    """

class syncSnapCreateError(OsvcException):
    """ Error in snapshot creation => clean up
    """

class syncSnapDestroyError(OsvcException):
    """ Error in snapshot destroy => clean up
    """

class syncSnapMountError(OsvcException):
    """ Error mounting fs => clean up
    """

class EndAction(OsvcException):
    """ End multi-resource action. Not an error.
    """

class ContinueAction(OsvcException):
    """ Abort current resource action, but proceed anyway
    """

class AbortAction(OsvcException):
    """ Abort multi-resource action
    """

class InitError(OsvcException):
    """ Resource initialisation error
    """

class ScsiPrNotsupported(OsvcException):
    """ Scsi persistent reservation is not supported
    """

class NotAvailable(OsvcException):
    """ Not available
    """

class NotSupported(OsvcException):
    """ Not supported
    """

class MissImpl(OsvcException):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpDevDown(OsvcException):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpConflict(OsvcException):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpAlreadyUp(OsvcException):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpNoActions(OsvcException):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
