"""
Define the Freezer class, instantiated as a Svc lazy attribute,
providing the methods to freeze, thaw a service and to test if
a service is frozen.
"""
import os
from env import Env
from utilities.naming import split_path


class Freezer(object):
    """
    The freezer class, instantiated as a Svc lazy attribute.
    Provides methods to freeze, thaw a service and to test if
    the service is frozen.
    """

    def frozen(self, strict=False):
        """
        If strict, return the mtime if the service frozen file flag is present.

        If not strict, return the mtime of either, in turn, the service frozen
        file if present, or the node frozen file mtime if present.

        Return 0 if the service is not frozen.
        """
        try:
            return os.path.getmtime(self.flag)
        except (OSError, IOError):
            pass
        if not strict:
            try:
                return os.path.getmtime(self.node_flag)
            except (OSError, IOError):
                pass
        return 0

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
        Return the mtime of the node frozen file if present.

        Return 0 if the node is not frozen.
        """
        try:
            return os.path.getmtime(self.node_flag)
        except (OSError, IOError):
            pass
        return 0

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
        self.node_flag = os.path.join(Env.paths.pathvar, "node", "frozen")
        if name == "node":
            self.flag = self.node_flag
        else:
            name, namespace, kind = split_path(name)
            if namespace:
                self.flag = os.path.join(Env.paths.pathvar, "namespaces", namespace, kind, name, "frozen")
            else:
                self.flag = os.path.join(Env.paths.pathvar, kind, name, "frozen")
