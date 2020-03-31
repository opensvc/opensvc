import json

import pytest

import commands.pool


@pytest.mark.ci
@pytest.mark.usefixtures('has_euid_0', 'osvc_path_tests')
class TestPool:
    @staticmethod
    @pytest.mark.parametrize('argv',
                             (['ls', '--debug'],
                              ['status', '--debug'],
                              ['status', '--verbose', '--debug']),
                             ids=['ls', 'status', 'status --verbose'])
    def test_actions_return_code_is_0(argv):
        assert commands.pool.main(argv=argv) == 0

    @staticmethod
    def test_has_pool_with_known_type(tmp_file, capture_stdout):
        with capture_stdout(tmp_file):
            assert commands.pool.main(argv=['status', '--format', 'json']) == 0
        with open(tmp_file) as json_file:
            pools = json.load(json_file).values()
            assert len([pool for pool in pools if pool['type'] != 'unknown']) > 0
