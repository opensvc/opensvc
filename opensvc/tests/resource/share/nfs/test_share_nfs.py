import pytest

from tests.helpers import assert_resource_has_mandatory_methods


OS_LIST = {'HP-UX', 'Linux', 'SunOS'}

SCENARIOS = [
    ('share.nfs', {'rid': '#1', 'path': '/something', 'opts': ''}, 'share.nfs'),
]


@pytest.fixture(scope='function')
def share_capability(mocker):
    return mocker.patch('core.capabilities.capabilities',
                        new=['node.x.exportfs', 'node.x.share'])


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('share_capability')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('scenario', SCENARIOS)
class TestDriverShareNfsInstances:
    @staticmethod
    def test_has_correct_type(create_driver_resource, sysname, scenario):
        assert create_driver_resource(sysname, scenario).type == scenario[2]

    @staticmethod
    def test_has_mandatory_methods(create_driver_resource, sysname, scenario):
        assert_resource_has_mandatory_methods(create_driver_resource(sysname, scenario))
