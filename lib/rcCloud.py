import socket
import rcExceptions as ex

class Cloud(object):
    def __init__(self, s, auth):
        self.cid = s
        self.auth = auth

    def list_svcnames(self):
         print("todo")
         return []

    def list_nodes(self):
        try:
            nodes = self.driver.list_nodes()
        except socket.error as e:
            raise ex.excError("error connecting to %s cloud url (%s)"%(self.cid, str(e)))
        return nodes

