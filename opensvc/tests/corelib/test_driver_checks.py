import pytest

from drivers.check import Checks


@pytest.mark.ci
class TestChecks(object):
    @staticmethod
    def test_ensure_no_check_list_leak():
        check_list_count = len(Checks().check_list)
        for i in range(4):
            checks = Checks()
            assert len(checks.check_list) == check_list_count
        assert check_list_count > 0
