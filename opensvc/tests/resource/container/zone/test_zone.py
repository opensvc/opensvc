import pytest

from drivers.resource.container.zone import ContainerZone


LIB_CLASS = 'drivers.resource.container.zone.ContainerZone'


@pytest.fixture(scope='function')
def svc(mocker, tmp_path):
    svc = mocker.Mock(name='svc')
    svc.namespace = ''
    svc.name = 's'
    svc.loggerpath = 'something'
    svc.var_d = tmp_path
    return svc


@pytest.fixture(scope='function')
def zone(svc):
    zone = ContainerZone(rid='container#1', zonepath='/z/zonepath_kw')
    zone.svc = svc
    return zone


@pytest.fixture(scope='function')
def with_installed_zone(mocker):
    zone_data = dict(state='installed', zonepath='/z/something', brand='solaris')
    mocker.patch.object(ContainerZone, 'zone_data', zone_data)
    return zone_data


@pytest.fixture(scope='function')
def zone_configure(mocker):
    return mocker.patch.object(ContainerZone, 'zone_configure',
                               return_value=mocker.Mock('zone_configure'))


@pytest.mark.ci
@pytest.mark.usefixtures('with_installed_zone')
class TestContainerZoneProvisionWhenZoneIsInstalled:
    @staticmethod
    def test_returns_without_actions(zone_configure, zone):
        zone.provisioner()
        assert zone_configure.call_count == 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestContainerZonepath:
    @staticmethod
    def test_is_value_from_zoneadm(zone, with_installed_zone):
        with_installed_zone['zonepath'] = '/z/fromzoneadm'
        assert zone.zonepath == '/z/fromzoneadm'

    @staticmethod
    def test_fallback_to_zonecfg(mocker, zone):
        """Not sure this is possible issue"""
        mocker.patch.object(ContainerZone, 'get_zonepath_from_zonecfg_cmd',
                            return_value='/z/zonepath_from_zonecfg')
        assert zone.zonepath == '/z/zonepath_from_zonecfg'

    @staticmethod
    def test_fallback_to_zonecfg_exported(mocker, zone):
        mocker.patch.object(ContainerZone, 'get_zonepath_from_zonecfg_export',
                            return_value='/z/zonecfg_exported')
        assert zone.zonepath == '/z/zonecfg_exported'

    @staticmethod
    def test_fallback_to_resource_zonepath_kw(zone):
        assert zone.zonepath == '/z/zonepath_kw'

    @staticmethod
    def test_fallback_to_none(svc):
        zone = ContainerZone(rid='container#1')
        zone.svc = svc
        assert zone.zonepath is None