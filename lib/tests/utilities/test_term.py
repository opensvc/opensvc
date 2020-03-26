import os

import pytest

from utilities.render.term import term_width


@pytest.mark.ci
class TestTermWidth:
    @staticmethod
    def test_return_value_greater_than_78():
        assert term_width() >= 78

    @staticmethod
    def test_with_python_ge_3_3(mocker):
        if getattr(os, 'get_terminal_size', None) is None:
            pytest.skip('this test required os.get_terminal_size')
        mocker.patch.object(os, 'get_terminal_size', return_value=mocker.Mock(columns=100), autospec=True)
        assert term_width() == 100

    @staticmethod
    def test_with_python_lt_3_3(mocker):
        if getattr(os, 'get_terminal_size', None) is not None:
            mocker.patch.object(os, 'get_terminal_size', side_effect=OSError, autospec=True)
        mocker.patch('utilities.render.term.justcall', return_value=('columns 101;', '', 0), autospec=True)
        assert term_width() == 101

    @staticmethod
    def test_fallback_to_env_columns(mocker):
        if getattr(os, 'get_terminal_size', None) is not None:
            mocker.patch.object(os, 'get_terminal_size', side_effect=OSError, autospec=True)
        mocker.patch('utilities.proc.justcall', return_value=('', '', 1), autospec=True)
        mocker.patch.dict(os.environ, {'COLUMNS': '102'})
        assert term_width() == 102

    @staticmethod
    def test_return_default_78_when_no_other_values_are_possible(mocker):
        if getattr(os, 'get_terminal_size', None) is not None:
            mocker.patch.object(os, 'get_terminal_size', side_effect=OSError, autospec=True)
        mocker.patch('utilities.proc.justcall', return_value=('columns 0;', '', 0), autospec=True)
        mocker.patch.dict(os.environ, {'COLUMNS': '0'})
        assert term_width() == 78
