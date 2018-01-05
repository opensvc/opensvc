from freezer import Freezer

class TestFreezer:

    @classmethod
    def setup_class(cls):
        Freezer("node").freeze()
        Freezer("unittest").thaw()

    @classmethod
    def teardown_class(cls):
        Freezer("node").thaw()

    def test_01(self):
        """
        Freezer, not strict
        """
        ret = Freezer("unittest").frozen()
        assert ret == 1

    def test_02(self):
        """
        Freezer, strict
        """
        ret = Freezer("unittest").frozen(strict=True)
        assert ret == 0

