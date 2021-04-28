import pytest


@pytest.mark.ci
def test_can_use_set_process_title():
    from utilities.process_title import set_process_title
    set_process_title("om mon")
