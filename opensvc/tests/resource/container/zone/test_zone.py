import pytest

from drivers.resource.container.zone import ContainerZone


LIB_CLASS = 'drivers.resource.container.zone.ContainerZone'


@pytest.fixture(scope='function')
def svc(mocker):
    svc = mocker.Mock(name='svc')
    svc.namespace = ''
    svc.name = 's'
    svc.loggerpath = 'something'
    return svc


@pytest.fixture(scope='function')
def zone(svc):
    zone = ContainerZone(rid='container#1')
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


@pytest.mark.zone
@pytest.mark.ci
@pytest.mark.usefixtures('with_installed_zone')
class TestContainerZoneProvisionWhenZoneIsInstalled:
    @staticmethod
    def test_returns_without_actions(zone_configure, zone):
        zone.provisioner()
        assert zone_configure.call_count == 0
