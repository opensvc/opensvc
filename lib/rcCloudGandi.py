import rcCloud
import rcExceptions as ex
import socket

try:
    from libcloud.compute.types import Provider
    from libcloud.compute.providers import get_driver
    import libcloud.security
except ImportError:
    raise ex.excInitError("apache-libcloud module must be installed")

class Cloud(rcCloud.Cloud):
    mode = 'gandi'

    def __init__(self, s, auth):
        rcCloud.Cloud.__init__(self, s, auth)
        if 'key' not in auth:
            raise ex.excInitError("option 'apikey' is mandatory in gandi section")
        gandi = get_driver(Provider.GANDI)
        self.driver = gandi(auth['key'])

    def app_id(self):
        return ''

    def cloud_id(self):
        return mode

    def app_cloud_id(self):
        return mode

    def list_svcnames(self):
        l = []
        _id = self.app_cloud_id()
        try:
            vapps = self.driver.list_nodes()
        except socket.error, e:
            raise ex.excExecError("error connecting to %s cloud manager"%s)
        for vapp in vapps:
            __id = '.'.join((vapp.name, _id))
            for vm in vapp.extra['vms']:
                svcname = '.'.join((vm['name'], __id))
                l.append((vm['name'], svcname))
        return l

