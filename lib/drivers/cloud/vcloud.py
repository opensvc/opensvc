import core.cloud
import core.exceptions as ex
import socket

try:
    from libcloud.compute.types import Provider
    from libcloud.compute.providers import get_driver
    #import libcloud.security
except ImportError:
    raise ex.InitError("apache-libcloud module must be installed")

class Cloud(core.cloud.BaseCloud):
    mode = 'vcloud'

    def __init__(self, s, auth):
        super(Cloud, self).__init__(s, auth)
        if 'username' not in auth:
            raise ex.InitError("option 'username' is mandatory in vcloud section")
        if 'password' not in auth:
            raise ex.InitError("option 'password' is mandatory in vcloud section")
        if 'manager' not in auth:
            raise ex.InitError("option 'manager' is mandatory in vcloud section")
        if 'api_version' not in auth:
            auth['api_version'] = '1.5'
        vcloud = get_driver(Provider.VCLOUD)
        self.driver = vcloud(auth['username'], auth['password'],
                             host=auth['manager'], api_version=auth['api_version'])

    def app_id(self, name):
        return name.rstrip(self.auth['manager']).split('.')[-2]

    def cloud_id(self):
        return self.auth['manager']

    def app_cloud_id(self):
        _id = []
        l = self.auth['username'].split('@')
        if len(l) == 2:
            _id.append(l[1])
        _id.append(self.auth['manager'])
        return '.'.join(_id)

    def list_names(self):
        l = []
        _id = self.app_cloud_id()
        try:
            vapps = self.driver.list_nodes()
        except socket.error as e:
            raise ex.Error("error connecting to %s cloud manager" % self.cid)
        for vapp in vapps:
            __id = '.'.join((vapp.name, _id))
            for vm in vapp.extra['vms']:
                name = '.'.join((vm['name'], __id))
                l.append((vm['name'], name))
        return l

