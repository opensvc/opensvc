import pytest

from tests.helpers import assert_resource_has_mandatory_methods


# HP-UX not yet added because of leg_mpath_disable call during __init__()
OS_LIST = {'FreeBSD', 'OSF1', 'Linux', 'SunOS'}

SCENARIOS = [
    ('disk.scsireserv', {'rid': '#1'}, 'disk.scsireserv'),
]


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('scenario', SCENARIOS)
class TestDriverDiskScsireservInstances:
    @staticmethod
    def test_has_correct_type(create_driver_resource, sysname, scenario):
        assert create_driver_resource(sysname, scenario).type == scenario[2]

    @staticmethod
    def test_has_mandatory_methods(create_driver_resource, sysname, scenario):
        assert_resource_has_mandatory_methods(create_driver_resource(sysname, scenario))
