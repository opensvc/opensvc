import core.cloud
import core.exceptions as ex

try:
    from libcloud.compute.types import Provider
    from libcloud.compute.providers import get_driver
    import libcloud.security
except ImportError:
    raise ex.InitError("apache-libcloud module must be installed")

class Cloud(core.cloud.BaseCloud):
    mode = 'openstack'

    def __init__(self, s, auth):
        super(Cloud, self).__init__(s, auth)
        kwargs = {}

        if 'username' not in auth:
            raise ex.InitError("option 'username' is mandatory in %s section"%self.mode)

        if 'password' not in auth:
            raise ex.InitError("option 'password' is mandatory in %s section"%self.mode)

        if 'url' not in auth:
            raise ex.InitError("option 'url' is mandatory in %s section"%self.mode)

        kwargs['ex_force_auth_url'] = auth['url']

        if 'tenant' in auth:
            self.tenant_name = auth['tenant']
            kwargs['ex_tenant_name'] = auth['tenant']
        else:
            self.tenant_name = None

        if 'version' in auth:
            kwargs['ex_force_auth_version'] = auth['version']
        else:
            kwargs['ex_force_auth_version'] = '2.0_password'

        if 'service_name' in auth:
            kwargs['ex_force_service_name'] = auth['service_name']

        if 'verify_ssl_cert' in auth and not auth['verify_ssl_cert']:
            libcloud.security.VERIFY_SSL_CERT = False

        openstack = get_driver(Provider.OPENSTACK)
        self.driver = openstack( auth['username'], auth['password'], **kwargs)

    def app_id(self, name=None):
        return self.tenant_name

    def cloud_id(self):
        return self.auth['url'].split("/")[2].split(':')[0]

    def app_cloud_id(self):
        _id = []
        app_id = self.app_id()
        if app_id is not None:
            _id.append(app_id)
        _id.append(self.cloud_id())
        return '.'.join(_id)

    def list_names(self):
        l = []
        _id = self.app_cloud_id()
        for node in self.list_nodes():
            name = '.'.join((node.name, _id))
            l.append((node.name, name))
        return l

