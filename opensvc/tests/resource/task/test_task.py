import pytest

from drivers.resource.task.docker import TaskDocker
from drivers.resource.task.host import TaskHost
from drivers.resource.task.podman import TaskPodman


@pytest.mark.ci
@pytest.mark.parametrize('klass', [TaskHost, TaskPodman, TaskDocker])
class TestTaskHost:
    @staticmethod
    def test_it_defines_optional_to_true_by_default(klass):
        assert klass().optional

    @staticmethod
    def test_it_can_define_optional_to_false(klass):
        assert klass(optional=False).optional is False
