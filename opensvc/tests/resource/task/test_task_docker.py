import pytest

from drivers.resource.task.docker import TaskDocker


@pytest.mark.ci
class TestTaskDocker:
    @staticmethod
    def test_it_defines_timeout_from_timeout_kw():
        assert TaskDocker(timeout=12).start_timeout == 12
        assert TaskDocker().start_timeout == 0
        assert TaskDocker(start_timeout=9).start_timeout == 0
        assert TaskDocker(start_timeout=9, timeout=12).start_timeout == 12
