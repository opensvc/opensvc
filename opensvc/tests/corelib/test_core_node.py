import pytest

from core.node import Node

try:
    # noinspection PyCompatibility
    from unittest.mock import call
except ImportError:
    # noinspection PyUnresolvedReferences
    from mock import call


@pytest.mark.ci
class TestNode:
    @staticmethod
    @pytest.mark.parametrize('split_action, expected_call', [
        ('crash', 'sys_crash'),
        ('reboot', 'sys_reboot'),
    ])
    def test_suicide_call_correct_node_method(mocker, split_action, expected_call):
        mocker.patch.object(Node, expected_call)
        node = Node()
        node.suicide(method=split_action, delay=6)
        getattr(node, expected_call).assert_called_once_with(6)

    @staticmethod
    def test_suicide_with_invalid_method_log_warning(mocker):
        node = Node()
        mocker.patch.object(node.log, 'warning')
        node.suicide(method="invalid_split_action", delay=6)
        expected_warning_calls = [
            call('invalid commit suicide method %s', 'invalid_split_action'),
        ]
        node.log.warning.assert_has_calls(expected_warning_calls)
