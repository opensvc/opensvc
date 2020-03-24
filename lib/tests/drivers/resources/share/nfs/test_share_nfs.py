import pytest

from tests.drivers.resources.helpers import assert_resource_has_mandatory_methods


OS_LIST = {'HP-UX', 'Linux', 'SunOS'}

SCENARIOS = [
    ('share.nfs', 'NfsShare', {'rid': '#1', 'path': '/something', 'opts': ''}, 'share.nfs'),
]


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'which')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('scenario', SCENARIOS)
class TestDriverShareNfsInstances:
    @staticmethod
    def test_has_correct_type(create_driver_resource, sysname, scenario):
        assert create_driver_resource(sysname, scenario).type == scenario[3]

    @staticmethod
    def test_has_mandatory_methods(create_driver_resource, sysname, scenario):
        assert_resource_has_mandatory_methods(create_driver_resource(sysname, scenario))
