from __future__ import print_function

from rcStatus import Status, colorize_status, status_value, status_str
import rcColor

rcColor.use_color = "no"

class TestStatus:
    def test_colorize_status(self):
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

    def test_status_value(self):
        """
        Status, invalid status numeric value
        """
        ret = status_value("foo")
        assert ret is None

    def test_status_str(self):
        """
        Status, invalid status string
        """
        ret = status_str(40)
        assert ret is None

    def test_status_class(self):
        """
        Status, invalid init args
        """
        try:
            sta1 = Status("foo", "up")
        except Exception:
            return
        assert False

    def test_status_hash(self):
        """
        Status, use as hash key
        """
        data = {}
        sta1 = Status()
        data[sta1] = True
        assert data[sta1] == True

    def test_status_ops(self):
        """
        Status, ops
        """
        sta1 = Status()
        sta2 = Status("up")
        assert str(sta1 + sta2) == "up"

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
