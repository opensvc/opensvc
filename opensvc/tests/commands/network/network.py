import json

import pytest

import commands.network


@pytest.mark.ci
@pytest.mark.usefixtures('has_euid_0', 'osvc_path_tests')
class TestNetworkLs:
    @staticmethod
    def test_returns_0():
        assert commands.network.main(argv=["ls"]) == 0
        assert commands.network.main(argv=["ls", "--format", "json"]) == 0

    @staticmethod
    def test_has_a_default_config(tmp_file, capture_stdout):
        with capture_stdout(tmp_file):
            assert commands.network.main(argv=["ls", "--format", "json", "--color", "no"]) == 0

        with open(tmp_file) as std_out:
            result = json.load(std_out)
        assert result['default']['config']
