import resources
from rcGlobalEnv import *
import rcStatus

class Loop(resources.Resource):
    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        return True

    def start(self):
        pass

    def stop(self):
        pass

    def status(self, verbose=False):
        if self.is_up():
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def __init__(self, file, disabled=False, tags=set([]), optional=False):
        self.file = file
        resources.Resource.__init__(self, disabled=disabled, tags=tags, optional=optional)
