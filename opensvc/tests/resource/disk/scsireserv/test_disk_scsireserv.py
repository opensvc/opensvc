import pytest

from drivers.resource.disk.scsireserv.sg import mpathpersist_enabled_in_conf
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


@pytest.mark.ci
@pytest.mark.parametrize('config, expected', [
    [' reservation_key file', True],
    ['reservation_key file', True],
    ['reservation_key file ', True],
    ['reservation_key  file ', True],
    ['reservation_key\tfile', True],
    ['reservation_key\tfile\t', True],
    ['reservation_key "file"', True],
    ['reservation_key \t"file"', True],
    ['reservation_key\t"file"', True],
    ['reservation_key\t"file" ', True],
    ['reservation_key\t"file"\t', True],
    ['reservation_key\t"file"\t ', True],
    ['reservation_key \t"file"\t ', True],
    ['reservation_key "file" ', True],
    ['reservation_key  "file" ', True],
    ['reservation_key  "file"  ', True],
    ['reservation_key    "file"     ', True],

    ['', False],
    ['\n\n', False],
    ['_reservation_key file', False],
    ['reservation_key file a', False],
    ['reservation_key file a ', False],
    ['reservation_key "file" a', False],
    ['reservation_key "file" a ', False],
    ['reservation_key "file', False],
    ['reservation_key "file ', False],
    ['reservation_key  "file ', False],
    ['reservation_key file"', False],
    ['reservation_key file" ', False],
    ['reservation_key filea', False],
    ['reservation_key file-', False],
    ['reservation_key file"', False],
    ['reservation_key  "filea" ', False],
])
class TestMpathPersistEnabledInConf:
    @staticmethod
    def test_mpathpersist_enabled_in_conf(config, expected):
        assert mpathpersist_enabled_in_conf(config) is expected, \
            "mpathpersist_enabled_in_conf expected %s when config is \n%s" % (expected, config)
