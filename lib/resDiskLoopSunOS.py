import resources
from rcGlobalEnv import *
import rcStatus

class Loop(resources.Resource):
    def is_up(self):
        """
        Returns True if the volume group is present and activated
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

    def __init__(self, file, **kwargs):
        resources.Resource.__init__(self, **kwargs)
        self.file = file
