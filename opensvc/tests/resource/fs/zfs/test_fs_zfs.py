import pytest

from tests.helpers import assert_resource_has_mandatory_methods


OS_LIST = {'Linux', 'SunOS', 'FreeBSD'}

SCENARIOS = [
    ('fs.zfs', {'rid': '#1', 'mount_point': '/tmp/plop', 'fs_type': 'plop',
                'mount_options': None, 'device': '/dev/zvol/...'},
     'fs'),
]


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('scenario', SCENARIOS)
class TestDriverFsZfsInstances:
    @staticmethod
    def test_has_correct_type(create_driver_resource, sysname, scenario):
        assert create_driver_resource(sysname, scenario).type == scenario[2]

    @staticmethod
    def test_has_mandatory_methods(create_driver_resource, sysname, scenario):
        assert_resource_has_mandatory_methods(create_driver_resource(sysname, scenario))
