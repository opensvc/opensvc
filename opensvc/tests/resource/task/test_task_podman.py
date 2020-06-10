import pytest

from drivers.resource.task.podman import TaskPodman


@pytest.mark.ci
class TestTaskPodman:
    @staticmethod
    def test_it_defines_timeout_from_timeout_kw():
        assert TaskPodman(timeout=12).start_timeout == 12
        assert TaskPodman().start_timeout == 0
        assert TaskPodman(start_timeout=9).start_timeout == 0
        assert TaskPodman(start_timeout=9, timeout=12).start_timeout == 12
