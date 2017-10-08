"""
Define the Freezer class, instanciated as a Svc lazy attribute,
providing the methods to freeze, thaw a service and to test if
a service is frozen.
"""
import os
from rcGlobalEnv import rcEnv

class Freezer(object):
    """
    The freezer class, instanciated as a Svc lazy attribute.
    Provides methods to freeze, thaw a service and to test if
    the service is frozen.
    """

    def frozen(self, strict=False):
        """
        Return True if the service frozen file flag is present.
        """
        if os.path.exists(self.flag):
            return True
        if not strict and os.path.exists(self.node_flag):
            return True
        return False

    def freeze(self):
        """
        Create the service frozen file flag.
        """
        if os.path.exists(self.flag):
            return
        flag_d = os.path.dirname(self.flag)
        if not os.path.exists(flag_d):
            os.makedirs(flag_d, 0o0755)
        open(self.flag, 'w').close()

    def thaw(self):
        """
        Remove the service frozen file flag.
        """
        if self.flag != self.node_flag and os.path.exists(self.flag):
            os.unlink(self.flag)

    def node_frozen(self):
        """
        Return True if the node frozen file flag is present.
        """
        if os.path.exists(self.node_flag):
            return True
        return False

    def node_freeze(self):
        """
        Create the node frozen file flag.
        """
        if os.path.exists(self.node_flag):
            return
        flag_d = os.path.dirname(self.node_flag)
        if not os.path.exists(flag_d):
            os.makedirs(flag_d, 0o0755)
        open(self.node_flag, 'w').close()

    def node_thaw(self):
        """
        Remove the node frozen file flag.
        """
        if not os.path.exists(self.node_flag):
            return
        os.unlink(self.node_flag)

    def __init__(self, name):
        self.node_flag = os.path.join(rcEnv.paths.pathvar, "node", "frozen")
        if name == "node":
            self.flag = self.node_flag
        else:
            self.flag = os.path.join(rcEnv.paths.pathvar, "services", name, "frozen")
