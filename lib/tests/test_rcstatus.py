import pytest

from core.status import UP, colorize_status, status_value, status_str, Status

color = "no"


@pytest.mark.ci
class TestStatus:
    @staticmethod
    def test_colorize_status():
        """
        Status, colorization
        """
        sta1 = Status()
        ret = colorize_status(sta1, lpad=0)
        assert ret == "undef"

        sta1 = 0
        ret = colorize_status(sta1, lpad=0)
        assert ret == "up"

        sta1 = None
        ret = colorize_status(sta1, lpad=0)
        assert ret == "undef"

        sta1 = "warn"
        ret = colorize_status(sta1, lpad=0)
        assert ret == "warn"

        sta1 = "n/a"
        ret = colorize_status(sta1, lpad=0)
        assert ret == "n/a"

        sta1 = "unsupported"
        ret = colorize_status(sta1, lpad=0)
        assert ret == "unsupported"

    @staticmethod
    def test_status_value():
        """
        Status, invalid status numeric value
        """
        ret = status_value("foo")
        assert ret is None

    @staticmethod
    def test_status_str():
        """
        Status, invalid status string
        """
        ret = status_str(40)
        assert ret is None

    @staticmethod
    def test_status_class():
        """
        Status, invalid init args
        """
        try:
            # noinspection PyArgumentList
            Status("foo", "up")
        except Exception:
            return
        assert False

    @staticmethod
    def test_status_hash():
        """
        Status, use as hash key
        """
        data = {}
        sta1 = Status()
        data[sta1] = True
        assert data[sta1] is True

    @staticmethod
    def test_status_ops():
        """
        Status, ops
        """
        sta1 = Status()
        sta2 = Status("up")
        sta3 = Status("undef")
        sta4 = Status("down")
        sta5 = Status().status = 444
        assert str(sta1 + sta2) == "up"
        assert str(sta2 + sta3) == "up"
        assert str(sta2 + sta4) == "warn"
        assert Status("up").value() == UP

        sta6 = Status("up")
        sta6.reset()
        assert str(sta6) == "undef"

        assert Status("up") == Status("up")
        assert Status("down") != Status("up")
        assert Status("down") != UP
        assert UP != Status("down")
        assert "up" != Status("down")
        assert "down" == Status("down")
        assert UP == Status("up")
        assert Status("down") != "up"
        assert Status("down") == "down"

        try:
            sta2 += sta5
            assert False
        except AssertionError:
            raise
        except Exception:
            pass

        try:
            sta5 += sta2
            assert False
        except AssertionError:
            raise
        except Exception:
            pass

        try:
            sta2 += "foo"
            assert False
        except Exception:
            assert True

        sta1.status = "corrupt"
        try:
            sta1 += sta2
            assert False
        except Exception:
            assert True
