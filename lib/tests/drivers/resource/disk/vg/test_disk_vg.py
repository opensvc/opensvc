import pytest

from tests.helpers import assert_resource_has_mandatory_methods


OS_LIST = ['AIX', 'Linux', 'HP-UX']

SCENARIOS = [
    ('disk.vg', 'DiskVg', {'name': 'vg1'}, 'disk.vg'),
]


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('scenario', SCENARIOS)
class TestDriverDiskVgInstances:
    @staticmethod
    def test_has_correct_type(create_driver_resource, sysname, scenario):
        assert create_driver_resource(sysname, scenario).type == scenario[3]

    @staticmethod
    def test_has_mandatory_methods(create_driver_resource, sysname, scenario):
        assert_resource_has_mandatory_methods(create_driver_resource(sysname, scenario))
