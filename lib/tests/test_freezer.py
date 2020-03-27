import pytest

from core.freezer import Freezer


@pytest.fixture(scope='function')
def freezer(osvc_path_tests):  # auto inject osvc_path_tests, when freezer fixture is used
    return Freezer('test-svc')


@pytest.mark.ci
def test_frozen_node_become_not_node_frozen_after_node_thaw(freezer):
    freezer.node_freeze()
    assert freezer.node_frozen()
    freezer.node_thaw()
    assert not freezer.node_frozen()


@pytest.mark.ci
def test_node_frozen_after_node_freeze(freezer):
    assert not freezer.node_frozen()
    freezer.node_freeze()
    assert freezer.node_frozen()


@pytest.mark.ci
@pytest.mark.parametrize('node_freeze,svc_freeze,strict,frozen_result', [
    (True, True, False, True),
    (True, False, False, True),
    (False, True, False, True),
    (True, True, True, True),
    (True, False, True, False),
    (False, True, True, True),
])
def test_frozen_value_based_on_node_frozen_state_and_service_frozen_state_and_strict_value(
        freezer,
        node_freeze, svc_freeze, strict, frozen_result):
    if node_freeze:
        freezer.node_freeze()
    else:
        freezer.node_thaw()
    if svc_freeze:
        freezer.freeze()
    else:
        freezer.thaw()

    frozen = freezer.frozen(strict=strict)
    if frozen_result:
        assert frozen
    else:
        assert not frozen
